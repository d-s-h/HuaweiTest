/*
# High performance file duplicates finder

## Task descriptions

Consider N-core machines, and given folder path find all identical (by content) files under it.
I.e. implement a function:
```c++
std::vector<std::vector<std::string>> findIdentical(const string& path) {
    ....
}
```
which returns list of lists, where inner list is the list of all file names with coinciding content:
```
dir1
  |- dir2
  |  |- file2
  |- file1
  |- file3
  |- file4
```
if content of file2 == content of file3, return list like this: `[[dir2/file2, file3], [file1], [file4]]`.

## Special considerations

Please ensure that code uses concurrent computations as much as possible (assume, that IO backend is capable to serve K concurrent IO requests at the same time)
and works well for both large and small files.
*/

/*
High-level overview of the program.

* Get a list of all files and their sizes
* Use content compare for same size files only.
* To compare files content feed IOCP with Min(K, N) threads for optimal performance.
* Hash files content once read so compare next files by content only if hash/size are different
* Do I need collision handling: compare by content if hash/size are the same? Heuristics?

Feed IOCP with K files.

Threads to spawn:
Min(K, N) for optimal performance.
If N < K, consider spawning less threads but keep more requests? Not sure how to better handle this using high-level IOCP.

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
#include <tchar.h>
#include <locale>
#include <codecvt>
#include <vector>
#include <unordered_map>
#include <cassert>
#include <algorithm>

#include "Platform.h"
#include "Hash.h"

HashFunction* gHashFunction = MurmurHash64A;

bool operator== (SizeHashKey const& lhs, SizeHashKey const& rhs)
{
  return (lhs.size == rhs.size) && (lhs.hash == rhs.hash);
}

void setHashFunction(HashFunction* func)
{
  gHashFunction = func;
}

class BlockPrinter
{
public:
  ~BlockPrinter() = default;

  virtual void operator()(const uint8_t* block, const size_t size)
  {
    std::cout << "Read " << size << " bytes: " << std::endl;
    std::cout.write(reinterpret_cast<const char*>(block), size);
    std::cout << std::endl;
  }
};

class FileHasher : public IBlockReadCallback
{
public:
  FileHasher()
  {
  }

  void operator()(const uint8_t* block, size_t size) override
  {
    mHash += gHashFunction(block, static_cast<int>(size), 1234);
  }

  uint64_t getHash() { return mHash; }

private:
  uint64_t mHash = 0;
};

constexpr int MAX_CONCURRENT_IO = 8;

constexpr int FILE_BLOCK_SIZE = 4 * 1024 * 1024; // 4 MB

class MemBlockAllocator
{
public:
  MemBlockAllocator(uint64_t blockSize, uint64_t count)
  {
    mMemBlockPool.resize(blockSize * count);
    mFreeBlockMask.resize(count, true);
  }

private:
  uint64_t mBlockSize;
  uint64_t mTotalBlockCount;
  std::vector<uint8_t> mMemBlockPool;
  std::vector<bool> mFreeBlockMask;
};

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

// Yes, the copy elision is in place, but I'd like to improve API design of the func to pass the result back not as a copy.
std::vector<std::vector<std::string>> findIdentical(const std::string& path)
{/*
  *Gather buckets of files with the same size
    * If a bucket contains more than 2 files, then start a content compare multithreaded procedure.
    * Calculate hash for each file in the bucket.
    * Group files by hash
    * Extra step : make subgrouping by exact content compare
    * Write out file groups and subgroups if any.
    * Take next bucket
    * Extra step : spawn separate threads for each bucket and feed IOCP from multiple buckets.
    */

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

  std::cout << "Calculating hashes for " << map.size() << " files..." << std::endl;
  size_t fileHashProcessed = 0;
  for (size_t bucket = 0; bucket < map.bucket_count(); ++bucket)
  {
    // Optimize: ignore files when no other files with the same size.
    if(map.bucket_size(bucket) > 1)
    {
      for (auto it = map.begin(bucket); it != map.end(bucket); ++it)
      {
        FileInfo& fi = it->second;

        if (fi.size > 0)
        {
          FileHasher fileHasher;

          size_t totalBytesRead = readFile(fi.name, fileHasher);
          assert(totalBytesRead == fi.size);
          fi.contentHash = fileHasher.getHash();
          fi.hashed = true;
        }
        else
        {
          // Special case for empty files
          fi.contentHash = 0;
          fi.hashed = true;
        }
        ++fileHashProcessed;
        int progress = static_cast<int>(100.0f * fileHashProcessed / map.size());
        std::cout << "\rProgress " << progress << "%";
      }
    }
    else
    {
      fileHashProcessed += map.bucket_size(bucket);
      int progress = static_cast<int>(100.0f * fileHashProcessed / map.size());
      std::cout << "\rProgress " << progress << "%";
    }
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

  size_t contentCompareProcessed = 0;
  for (auto& it : fileHashMap)
  {
    SizeHashEntry& e = it.second;
    int i = 0;
    if (e.files.size() > 1 && e.files[0]->size > 0)
    {
      findDupContent(e.files, e.multiSet);
      int progress = static_cast<int>(100.0f * contentCompareProcessed / contentCompareGroups);
      std::cout << "\rProgress " << progress << "%";
      ++contentCompareProcessed;
    }
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

              std::cout << ((i != 0) ? "  " : "") << narrow << " size " << fi->size << " hash " << std::hex << fi->contentHash << std::dec << std::endl;
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
            std::cout << narrow << " size " << fi->size << " hash " << std::hex << fi->contentHash << std::dec << std::endl;
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
        std::cout << narrow << " size " << fi->size << " hash " << std::hex << fi->contentHash << std::dec << std::endl;
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
