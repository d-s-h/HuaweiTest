/*

Assumptions:
*
*
*

Algorithm:
* Gather buckets of files with the same size
* If a bucket contains more than 2 files, then start a content compare multithreaded procedure.
* Calculate hash for each file in the bucket.
* Group files by hash
* Extra step: make subgrouping by exact content compare
* Write out file groups and subgroups if any.
* Take next bucket
* Extra step: spawn separate threads for each bucket and feed IOCP from multiple buckets.

Content compare algorithm for identical size/hash files (called a subgroup):
* Split files by K sets.
* Sort files in each set using up to K concurrent IO requests
* Do logN set merges using by up to K concurrent IO requests

Further improvements:
* Use MD5 hash
* Use block-chain (e.g. hashing of blocks split by page size) to avoid full file content comparison. Not worth usuing because files with the same hash/size are usually the same.
* Better path handling
* Multiplatform support
* Open source
*/
#include "DupFinder.h"

#include <iostream>
#include <chrono>
#include <thread>
#include <locale>
#include <codecvt>
#include <vector>
#include <unordered_map>
#include <cassert>
#include <algorithm>
#include <mutex>

#include "Platform.h"
#include "ThreadPool.h"
#include "IOPool.h"
#include "Hash.h"

const int WORKER_THREADS = 1;
const int CONCURRENT_IO = 1;
const int FILE_BLOCK_SIZE = 16 * 1024 * 1024; // 4 MB

HashFunction* gHashFunction = MurmurHash64A;

class Profiler {
public:
  Profiler(const std::string& name) : name(name), startTime(std::chrono::high_resolution_clock::now()) {}

  ~Profiler()
  {
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
    float seconds = duration / 1000.0f;
    printf("%s took %.3f seconds\n", name.c_str(), seconds);
  }

private:
  std::string name;
  std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
};

bool operator== (SizeHashKey const& lhs, SizeHashKey const& rhs)
{
  return (lhs.size == rhs.size) && (lhs.hash == rhs.hash);
}

void setHashFunction(HashFunction* func)
{
  gHashFunction = func;
}

void findDupContent(const std::vector<const FileInfo*>& files, AsyncMultiSet& set)
{
  DataComparer comparer;

  std::vector<int> insertIdxList(files.size());

  for (int i = 0; i < files.size(); ++i)
  {
    insertIdxList[i] = i;
  }

  do
  {
    comparer.getQueue().clear();

    for (int i = 0; i < insertIdxList.size(); ++i)
    {
      set.insert(insertIdxList[i], &comparer);
    }

    insertIdxList.clear();
    for (auto& e : comparer.getQueue())
    {
      const FileInfo* fi1 = files[e.first];
      const FileInfo* fi2 = files[e.second];
      int res = 0;
      size_t bytesRead = compareFiles(fi1->name, fi2->name, res);
      comparer.addCompareResult(e.first, e.second, res);
      insertIdxList.push_back(e.first);
    }
  } while (comparer.getQueue().size() > 0);

}

class FileHasher
{
public:
  FileHasher(uint32_t blockSize, uint32_t bufferBlockCount, ThreadPool* threadPool, IOPool* ioPool):
    mBlockSize(blockSize), mBufferBlockCount(bufferBlockCount), mThreadPool(threadPool), mIOPool(ioPool)
  {
    assert(bufferBlockCount >= 2); // 2 blocks is minimum
    mFreeReadBufferBlocks.resize(bufferBlockCount, 0);
    mReadBuffer.resize(bufferBlockCount * blockSize);
  }

  void enqueue(FileInfo* fi)
  {
    mFiles.push_back(fi);
  }

  void calcHashes()
  {
    mProcessedFiles = 0;

    for(auto& fi : mFiles)
    {
      IOJob job;
      job.filename = fi->name;
      job.buffer = acquireReadBlock(OWNER_STAGE_SUBMIT, mBufferBlockCount / 2); // Limit buffer allocation so workers have ones
      job.bufferSize = mBlockSize;
      job.blockReadCallback = sReadBlockCallback;
      job.finishCallback = sReadFinishCallback;
      uint32_t blocksCount = static_cast<uint32_t>(fi->size / mBlockSize + (fi->size % mBlockSize != 0));
      BlockChain* bc = new BlockChain;
      bc->hashBlocks.resize(blocksCount);
      bc->blocksNotReady = blocksCount;
      job.ctx = new Context(this, fi, job.buffer, job.bufferSize, 0, bc);
      
      mIOPool->submitJob(job);

      int progress = static_cast<int>(100.0f * mProcessedFiles / mFiles.size());
      printf("\rProgress %d%%", progress);
    }
    printf("\r");

    // All files were sent to hash calculation, wait for results
    while (mProcessedFiles < mFiles.size())
    {
      mIOPool->waitWorkers();
      mThreadPool->waitWorkers();
    }
  }

private:
  static constexpr uint8_t OWNER_STAGE_SUBMIT = 1;
  static constexpr uint8_t OWNER_STAGE_READ = 2;

  using HashBlocks = std::vector<uint64_t>;
  struct BlockChain
  {
    HashBlocks hashBlocks;
    std::atomic<uint32_t> blocksNotReady;
  };

  struct Context
  {
    FileHasher* hasher = nullptr;
    FileInfo* fileInfo = nullptr;
    const uint8_t* block = nullptr;
    uint32_t size = 0;
    uint64_t readOffset = 0;
    BlockChain* blockChain = nullptr;
  };

  static IOStatus sReadBlockCallback(const uint8_t* block, const uint64_t bytesRead, void* ctx)
  {
    assert(ctx);
    Context* context = static_cast<Context*>(ctx);
    return context->hasher->readBlockCallback(block, static_cast<uint32_t>(bytesRead), context);
  }

  IOStatus readBlockCallback(const uint8_t* block, const uint32_t bytesRead, Context* ctx)
  {
    LOG("->FileHasher::readBlockCallback: block 0x%p\n", block);
    assert(ctx);
    assert(bytesRead <= mBlockSize);

    // Create a separate context for next job because of different buffer, pass the current data block
    Context* calcBlockHashCtx = new Context(this, ctx->fileInfo, block, bytesRead, ctx->readOffset, ctx->blockChain);
    mThreadPool->submitWork(sCalcBlockHashCallback, calcBlockHashCtx);

    // Keep track of read position to be able to determine hash block.
    ctx->readOffset += bytesRead;

    // Supply IO with a new buffer to read in
    IOStatus status;
    status.buffer = acquireReadBlock(OWNER_STAGE_READ,  0);
    status.bufferSize = mBlockSize;
    ctx->block = status.buffer; // Keep the new one to be released in the end.
    LOG("<-FileHasher::readBlockCallback\n");
    return status;
  }

  static void sReadFinishCallback(void* ctx)
  {
    assert(ctx);
    Context* context = static_cast<Context*>(ctx);
    context->hasher->readFinishCallback(context);
    assert(ctx);
  }

  void readFinishCallback(Context* ctx)
  {
    LOG("->FileHasher::readFinishCallback\n");
    assert(ctx);

    // Memory block isn't needed anymore for file reading
    releaseReadBlock(ctx->block);
    delete ctx;
    LOG("<-FileHasher::readFinishCallback\n");
  }

  static void sCalcBlockHashCallback(void* ctx)
  {
    assert(ctx);
    Context* context = static_cast<Context*>(ctx);
    context->hasher->calcBlockHashCallback(context);
  }

  void calcBlockHashCallback(Context* ctx)
  {
    LOG("->FileHasher::calcBlockHashCallback\n");
    assert(ctx);
    
    size_t blockIdx = ctx->readOffset / mBlockSize;
    assert(blockIdx < ctx->blockChain->hashBlocks.size());
    ctx->blockChain->hashBlocks[blockIdx] = gHashFunction(ctx->block, static_cast<int>(ctx->size), 1234);

    std::atomic<uint32_t>& blocksNotReady = ctx->blockChain->blocksNotReady;

    // Memory block isn't needed anymore for hash calc
    releaseReadBlock(ctx->block);
    ctx->block = nullptr;
    ctx->size = 0;

    // Check if it was the last block
    if (blocksNotReady.fetch_sub(1) == 1)
    {
      // The last block was calculated, it's time to reduce the results.
      mThreadPool->submitWork(sCalcFileHashCallback, ctx);
    }
    LOG("<-FileHasher::calcBlockHashCallback\n");
  }

  static void sCalcFileHashCallback(void* ctx)
  {
    assert(ctx);
    Context* context = static_cast<Context*>(ctx);
    context->hasher->calcFileHashCallback(context);
  }

  void calcFileHashCallback(Context* ctx)
  {
    LOG("->FileHasher::calcFileHashCallback\n");
    assert(ctx);
    assert(ctx->blockChain);
    for (const uint64_t blockHash : ctx->blockChain->hashBlocks)
    {
      ctx->fileInfo->contentHash += blockHash;
    }
    ctx->fileInfo->hashed = true;
    ++mProcessedFiles;

    // Release all context resources
    delete ctx->blockChain;
    delete ctx;
    LOG("<-FileHasher::calcFileHashCallback\n");
  }

  uint8_t* acquireReadBlock(uint8_t ownerId, int limit)
  {
    LOG("->FileHasher::acquireReadBlock\n");
    assert(ownerId > 0);
    std::unique_lock<std::mutex> lock(mMutex);

    // Wait until a block is available
    mCondition.wait(lock,
      [this, limit]()
      {
        return std::count(mFreeReadBufferBlocks.begin(), mFreeReadBufferBlocks.end(), 0) > limit;
      });

    uint8_t* block = nullptr;
    size_t idx = 0;
    auto it = std::find(mFreeReadBufferBlocks.begin(), mFreeReadBufferBlocks.end(), 0);
    if(it != mFreeReadBufferBlocks.end())
    {
      idx = it - mFreeReadBufferBlocks.begin();
      assert(mFreeReadBufferBlocks[idx] == 0);
      mFreeReadBufferBlocks[idx] = ownerId;
      block = &mReadBuffer[idx * mBlockSize];
    }
    else
    {
      assert(0);
    }
    LOG("<-FileHasher::acquireReadBlock: idx %lld 0x%p\n", idx, block);
    return block;
  }

  void releaseReadBlock(const uint8_t* block)
  {
    size_t idx = (block - mReadBuffer.data()) / mBlockSize;
    LOG("->FileHasher::releaseReadBlock: idx %lld 0x%p\n", idx, block);
    assert(idx >= 0 && idx < mFreeReadBufferBlocks.size());
    std::lock_guard<std::mutex> lock(mMutex);
    assert(mFreeReadBufferBlocks[idx] > 0);
    mFreeReadBufferBlocks[idx] = 0;
    mCondition.notify_one(); // notify that a block has been release so it can be acquired again.
    LOG("<-FileHasher::releaseReadBlock\n");
  }

  std::mutex mMutex;
  std::condition_variable mCondition;
  std::vector<FileInfo*> mFiles;
  ThreadPool* mThreadPool = nullptr;
  IOPool* mIOPool = nullptr;
  uint32_t mBufferBlockCount = 0;
  uint32_t mBlockSize;
  std::vector<uint8_t> mFreeReadBufferBlocks;
  std::vector<uint8_t> mReadBuffer;
  std::vector<bool> mFreeHashingBufferBlocks;
  std::vector<uint8_t> mHashingBuffer;
  std::atomic<int> mProcessedFiles;
};


// Yes, the copy elision is in place, but I'd like to improve API design of the func to pass the result back not as a copy.
std::vector<std::vector<std::string>> findIdentical(const std::string& path)
{
/*
  - Gather buckets of files with the same size
  - If a bucket contains more than 2 files, then start a content compare multithreaded procedure.
  - Calculate hash for each file in the bucket.
  - Group files by hash
  - Extra step : make subgrouping by exact content compare
  - Write out file groups and subgroups if any.
  - Take next bucket
  - Extra step : spawn separate threads for each bucket and feed IOCP from multiple buckets.
*/

  Profiler profileScope("findIdentical");
  assert(gHashFunction);
  std::vector<std::vector<std::string>> result;


  std::wstring oldDir;
  if (!getCurrentDir(oldDir))
  {
    return result;
  }

  std::wstring dir = oldDir;
  if (path.size() > 2)
  {
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    std::wstring widePath = converter.from_bytes(path);
    if (path[1] == ':')
    {
      // Absolute path
      dir = widePath;
    }
    else
    {
      // Relative path
      dir.append(L"\\");
      dir.append(widePath);
    }
  }

  if (!setCurrentDir(dir))
  {
    return result;
  }

  // Display the current working directory
  std::wcout << L"Directory: " << dir << std::endl;

  FileInfoMap map;
  getFileInfoRecursive(dir, map, L"");

  ThreadPool threadPool(WORKER_THREADS);
  IOPool ioPool(CONCURRENT_IO);
  FileHasher hasher(FILE_BLOCK_SIZE, (WORKER_THREADS + CONCURRENT_IO) * 2, &threadPool, &ioPool);

  std::cout << "Calculating hashes for " << map.size() << " files..." << std::endl;
  {
    Profiler profileScope("File hashes calculation");
    size_t calcHashJobs = 0;
    for (size_t bucket = 0; bucket < map.bucket_count(); ++bucket)
    {
      // Optimize: ignore files when no other files with the same size.
      if (map.bucket_size(bucket) > 1)
      {
        for (auto it = map.begin(bucket); it != map.end(bucket); ++it)
        {
          FileInfo& fi = it->second;

          if (fi.size > 0)
          {
            hasher.enqueue(&fi);
          }
          else
          {
            // Special case for empty files
            fi.contentHash = 0;
            fi.hashed = true;
          }
        }
      }
    }
    hasher.calcHashes();
  }

  // Sets of same size/hash files
  FileHashMap fileHashMap;
  for (size_t i = 0; i < map.bucket_count(); ++i)
  {
    for (auto it = map.cbegin(i); it != map.cend(i); ++it)
    {
      const FileInfo& fi = it->second;
      SizeHashKey shk;
      shk.size = fi.size;
      shk.hash = fi.contentHash;
      auto itFHM = fileHashMap.find(shk);
      
      if(itFHM != fileHashMap.end())
      {
        // A set is already there
        itFHM->second.files.push_back(&fi);
      }
      else
      {
        auto resPair = fileHashMap.insert(std::make_pair(shk, SizeHashEntry()));
        if (resPair.second)
        {
          resPair.first->second.files.push_back(&fi);
        }
        else
        {
          assert(0);
        }
      }
    }
  }

  // Compare content of files with the same size/hash (if there is more than 1 file and they're not empty)
  size_t contentCompareGroups = 0;
  for (auto& it : fileHashMap)
  {
    SizeHashEntry& e = it.second;
    if (e.files.size() > 1 && e.files[0]->size > 0)
    {
      ++contentCompareGroups;
    }
  }
  
  std::cout << "\rContent compare..." << std::endl;

  {
    Profiler profileScope("Content compare");
    size_t contentCompareProcessed = 0;
    for (auto& it : fileHashMap)
    {
      SizeHashEntry& e = it.second;
      int i = 0;
      if (e.files.size() > 1 && e.files[0]->size > 0)
      {
        findDupContent(e.files, e.multiSet);
        int progress = static_cast<int>(100.0f * contentCompareProcessed / contentCompareGroups);
        printf("\rProgress %d%%", progress);
        ++contentCompareProcessed;
      }
    }
    printf("\r");
  }

  std::cout << "\rForming result..." << std::endl;

  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;

  // Fill out result
  for (auto& it : fileHashMap)
  {
    SizeHashEntry& e = it.second;
    if (e.files.size() > 0)
    {
      if (e.files.size() > 1)
      {
        if(e.files[0]->size > 0)
        {
          // Dup content file indices are stored in the set
          AsyncSetIterator itSet = e.multiSet.getIterator();
          while (itSet.hasNext())
          {
            std::vector<std::string> dups;
            std::vector<int> indices = itSet.next();
            for (int i = 0; i < indices.size(); ++i)
            {
              int fileIdx = indices[i];
              const FileInfo* fi = e.files[fileIdx];
              std::string narrow = converter.to_bytes(fi->name);
              dups.emplace_back(narrow);

              LOG("%s%s size %llu hash %llx\n", ((i != 0) ? "  " : ""), narrow.c_str(), fi->size, fi->contentHash);
            }
            std::sort(dups.begin(), dups.end());
            result.emplace_back(dups);
          }
        }
        else
        {
          // Empty files are all considered as dups
          std::vector<std::string> dups;
          for (auto& fi : e.files)
          {
            std::string narrow = converter.to_bytes(fi->name);
            dups.emplace_back(narrow);
            LOG("%s size %llu hash %llx\n", narrow.c_str(), fi->size, fi->contentHash);
          }
          result.emplace_back(dups);
        }
      }
      else
      {
        const FileInfo* fi = e.files[0];
        std::string narrow = converter.to_bytes(fi->name);
        std::vector<std::string> fileNames = { narrow };
        result.emplace_back(fileNames);
        LOG("%s size %llu hash %llx\n", narrow.c_str(), fi->size, fi->contentHash);
      }
    }
  }

  std::sort(result.begin(), result.end(),
    [](const auto& l, const auto& r)
    {
      return l[0] < r[0];
    }
  );

  // Set the old dir back to avoid side effects on external code.
  setCurrentDir(oldDir);

  return result;
}
