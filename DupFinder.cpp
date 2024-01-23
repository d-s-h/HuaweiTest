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
#include <locale>
#include <codecvt>
#include <vector>
#include <unordered_map>
#include <cassert>
#include <algorithm>

#include "Platform.h"
#include "Hash.h"
#include "AsyncMultiSet.h"
#include "AsyncFileComparer.h"

const int FILE_BLOCK_SIZE = 16 * 1024 * 1024; // 16 MB

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

struct SizeHashKey
{
  uint64_t size = 0;
  uint64_t hash = 0;
};

struct SizeHashKeyHash
{
  size_t operator()(const SizeHashKey& key) const
  {
    return key.hash ^ key.size;
  }
};

struct SizeHashEntry
{
  SizeHashEntry() {}
  std::vector<const FileInfo*> files;
  AsyncMultiSet multiSet;
};

using FileHashMap = std::unordered_map<SizeHashKey, SizeHashEntry, SizeHashKeyHash>;

bool operator== (SizeHashKey const& lhs, SizeHashKey const& rhs)
{
  return (lhs.size == rhs.size) && (lhs.hash == rhs.hash);
}



void findDupContent(const std::vector<const FileInfo*>& files, AsyncMultiSet& set, ThreadPool& threadPool, IOPool& ioPool)
{
  DataComparer comparer;
  AsyncFileComparer fileComparer(FILE_BLOCK_SIZE, &threadPool, &ioPool);

  std::vector<int> insertIdxList(files.size());

  for (int i = 0; i < files.size(); ++i)
  {
    insertIdxList[i] = i;
  }

  std::vector<AsyncFileComparer::Result> results;
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
      fileComparer.enqueue(files[e.first], files[e.second]);

      //int res = 0;
      //size_t bytesRead = compareFiles(fi1->name, fi2->name, res);
    }

    fileComparer.getResults(results);

    for (const AsyncFileComparer::Result& res : results)
    {
      const AsyncFileComparer::FilePair& filePair = res.first;
      int result = res.second;

      auto it1 = std::find(files.begin(), files.end(), filePair.first);
      assert(it1 != files.end());
      int fileIdx1 = static_cast<int>(it1 - files.begin());
      
      auto it2 = std::find(files.begin(), files.end(), filePair.second);
      assert(it2 != files.end());
      int fileIdx2 = static_cast<int>(it2 - files.begin());

      comparer.addCompareResult(fileIdx1, fileIdx2, result);
      insertIdxList.push_back(fileIdx1);
    }
    results.clear();

  } while (comparer.getQueue().size() > 0 || insertIdxList.size() > 0);

}

DupFinder::DupFinder(int concurrentIO, int workerThreads):
  mThreadPool(workerThreads),
  mIoPool(concurrentIO),
  mHasher(FILE_BLOCK_SIZE, (workerThreads + concurrentIO) * 2, &mThreadPool, &mIoPool)
{
}

void DupFinder::setHashFunction(HashFunction* func)
{
  mHasher.setHashFunction(func);
}

// Yes, the copy elision is in place, but I'd like to improve API design of the func to pass the result back not as a copy.
std::vector<std::vector<std::string>> DupFinder::findIdentical(const std::string& path)
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
            mHasher.enqueue(&fi);
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
    mHasher.calcHashes();
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
        findDupContent(e.files, e.multiSet, mThreadPool, mIoPool);
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

  // Sort in the ascending order
  std::sort(result.begin(), result.end(), std::less<>());

  // Set the old dir back to avoid side effects on external code.
  setCurrentDir(oldDir);

  return result;
}

