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

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <iostream>
#include <tchar.h>
#include <locale>
#include <codecvt>
#include <vector>
#include <unordered_map>
#include <cassert>
#include <algorithm>

#include "DupFinder.h"
#include "Hash.h"

int gError = 0; // I could use exceptions or change func API to pass an error code but use the global for simplicity.
HashFunction* gHashFunction = MurmurHash64A;

struct Stats
{
  uint64_t totalBytesRead = 0;
};

Stats gStats;

// Define the size of the buffer for reading from the file
constexpr DWORD BUFFER_SIZE = 4096;

// Custom deleter for HANDLE resources
struct HandleDeleter
{
  void operator()(HANDLE h) const
  {
    CloseHandle(h);
  }
};

using UniqueHandle = std::unique_ptr<void, HandleDeleter>;

bool operator== (SizeHashKey const& lhs, SizeHashKey const& rhs)
{
  return (lhs.size == rhs.size) && (lhs.hash == rhs.hash);
}

// Structure to hold information about an asynchronous file operation
struct FileIOData
{
  FileIOData() : fileHandle(nullptr, HandleDeleter()) {}

  OVERLAPPED overlapped;
  UniqueHandle fileHandle;
  CHAR buffer[BUFFER_SIZE];
};

void setHashFunction(HashFunction* func)
{
  gHashFunction = func;
}

// Function to fill info for files in a directory recursively
void GetFileInfoRecursive(const std::wstring& directoryPath, FileInfoMap& fileInfoMap, const std::wstring relativePath) {
  WIN32_FIND_DATA findFileData;
  HANDLE hFind = FindFirstFile((directoryPath + L"\\*").c_str(), &findFileData);

  if (hFind == INVALID_HANDLE_VALUE) {
    return; // No files found
  }

  uint64_t totalSize = 0;

  do {
    if (_tcscmp(findFileData.cFileName, _T(".")) == 0 || _tcscmp(findFileData.cFileName, _T("..")) == 0) {
      continue; // Skip "." and ".." entries
    }

    if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
      // If it's a directory, recurse into it
      std::wstring subdirectoryPath = directoryPath + L"\\" + findFileData.cFileName;
      GetFileInfoRecursive(subdirectoryPath, fileInfoMap, relativePath + L"\\" + findFileData.cFileName);
    }
    else {
      ULARGE_INTEGER fileSize;
      fileSize.LowPart = findFileData.nFileSizeLow;
      fileSize.HighPart = findFileData.nFileSizeHigh;

      FileInfo fi;
      if(!relativePath.empty())
      {
        fi.name.append(relativePath);
        fi.name.append(L"\\");
      }
      fi.name.append(findFileData.cFileName);
      fi.size = fileSize.QuadPart;
      fileInfoMap.insert(std::make_pair(fi.size, std::move(fi))); // use movement to avoid string copy.
    }
  } while (FindNextFile(hFind, &findFileData) != 0);

  FindClose(hFind);
}

class IBlockReadCallback
{
public:
  ~IBlockReadCallback() = default;
  
  virtual void operator()(const uint8_t* block, const size_t size) = 0;
};

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

struct FileOp
{
  DWORD bytesRead = 0;
  ULONG_PTR key = NULL;
  LPOVERLAPPED overlapped = NULL;
};

constexpr int MAX_CONCURRENT_IO = 8;

FileOp gFileOpPool[MAX_CONCURRENT_IO];

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

size_t readFile(const std::wstring& path, IBlockReadCallback& cb)
{
  static MemBlockAllocator sBlockAllocator(FILE_BLOCK_SIZE, MAX_CONCURRENT_IO);
  size_t totalBytesRead = 0;
  // Create and initialize the data structure for I/O operations
  std::unique_ptr<FileIOData> ioData = std::make_unique<FileIOData>();

  // Open the file for asynchronous reading
  ioData->fileHandle.reset(CreateFile(
    path.c_str(),
    GENERIC_READ,
    FILE_SHARE_READ,
    nullptr,
    OPEN_EXISTING,
    FILE_FLAG_OVERLAPPED,
    nullptr
  ));

  if (ioData->fileHandle.get() == INVALID_HANDLE_VALUE)
  {
    std::cerr << "Error opening file." << std::endl;
    gError = 1;
    return totalBytesRead;
  }

  // Create an I/O Completion Port
  DWORD threadCount = 0;
  UniqueHandle completionPort(CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, threadCount), HandleDeleter());
  if (completionPort == nullptr)
  {
    std::cerr << "Error creating I/O Completion Port." << std::endl;
    gError = 1;
    return totalBytesRead;
  }

  // Associate the file handle with the completion port
  // NumberOfConcurrentThreads parameter is ignored if the ExistingCompletionPort parameter is not NULL.
  if (CreateIoCompletionPort(ioData->fileHandle.get(), completionPort.get(), 0, 0) == nullptr)
  {
    std::cerr << "Error associating file handle with I/O Completion Port." << std::endl;
    gError = 1;
    return totalBytesRead;
  }

  // Initialize the overlapped structure
  ZeroMemory(&ioData->overlapped, sizeof(OVERLAPPED));

  // Wait for completion of the asynchronous operation
  DWORD bytesRead = 0;
  ULONG_PTR key = NULL;
  LPOVERLAPPED overlapped = NULL;
  do
  {
    // Perform the asynchronous read operation
    if (!ReadFile(ioData->fileHandle.get(), ioData->buffer, BUFFER_SIZE, nullptr, &ioData->overlapped))
    {
      if (GetLastError() != ERROR_IO_PENDING)
      {
        std::cerr << "Error initiating asynchronous read." << std::endl;
        gError = 1;
        return totalBytesRead;
      }
    }

    if (GetQueuedCompletionStatus(completionPort.get(), &bytesRead, &key, &overlapped, INFINITE))
    {
      // Process the completed operation
      cb(reinterpret_cast<const uint8_t*>(ioData->buffer), bytesRead);
      totalBytesRead += bytesRead;

      overlapped->Offset += bytesRead;
      gStats.totalBytesRead += bytesRead;
    }
    else
    {
      bool success = false;
      // Check the result of the asynchronous read without waiting (forth parameter FALSE). 
      success = GetOverlappedResult(ioData->fileHandle.get(), overlapped, &bytesRead, FALSE);

      if (!success)
      {
        if (GetLastError() != ERROR_HANDLE_EOF)
        {
          std::cerr << "Error completing asynchronous read." << std::endl;
          gError = 1;
        }
        break;
      }
    }
  } while (bytesRead > 0);

  gError = 0;
  return totalBytesRead;
}

size_t compareFiles(const std::wstring& path1, const std::wstring& path2, int& compareResult)
{
  size_t totalBytesRead = 0;

  // Create an I/O Completion Port
  DWORD threadCount = 0;
  UniqueHandle completionPort(CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, threadCount), HandleDeleter());
  if (completionPort == nullptr)
  {
    std::cerr << "Error creating I/O Completion Port." << std::endl;
    gError = 1;
    return totalBytesRead;
  }

  // Create and initialize the data structure for I/O operations
  std::vector<std::unique_ptr<FileIOData>> ioData(2);
  const std::wstring* files[2] = { &path1, &path2 };

  for(int i = 0; i < ioData.size(); ++i)
  {
    ioData[i] = std::make_unique<FileIOData>();

    // Initialize the overlapped structure
    ZeroMemory(&ioData[i]->overlapped, sizeof(OVERLAPPED));

    // Open the file for asynchronous reading
    ioData[i]->fileHandle.reset(CreateFile(
      files[i]->c_str(),
      GENERIC_READ,
      FILE_SHARE_READ,
      nullptr,
      OPEN_EXISTING,
      FILE_FLAG_OVERLAPPED,
      nullptr
    ));

    if (ioData[i]->fileHandle.get() == INVALID_HANDLE_VALUE)
    {
      std::cerr << "Error opening file." << std::endl;
      gError = 1;
      return totalBytesRead;
    }

    // Associate the file handle with the completion port
    // NumberOfConcurrentThreads parameter is ignored if the ExistingCompletionPort parameter is not NULL.
    if (CreateIoCompletionPort(ioData[i]->fileHandle.get(), completionPort.get(), i, 0) == nullptr)
    {
      std::cerr << "Error associating file handle with I/O Completion Port." << std::endl;
      gError = 1;
      return totalBytesRead;
    }
  }

  // Wait for completion of the asynchronous operation
  DWORD bytesRead1 = 0;
  DWORD bytesRead2 = 0;
  ULONG_PTR key1 = NULL;
  ULONG_PTR key2 = NULL;
  LPOVERLAPPED overlapped1 = NULL;
  LPOVERLAPPED overlapped2 = NULL;
  compareResult = 0;
  do
  {
    // Perform the asynchronous read operation
    for(int i = 0; i < ioData.size(); ++i)
    {
      if (!ReadFile(ioData[i]->fileHandle.get(), ioData[i]->buffer, BUFFER_SIZE, nullptr, &ioData[i]->overlapped))
      {
        if (GetLastError() != ERROR_IO_PENDING)
        {
          std::cerr << "Error initiating asynchronous read." << std::endl;
          gError = 1;
          return totalBytesRead;
        }
      }
    }

    bool status1 = false;
    bool status2 = false;
    status1 = GetQueuedCompletionStatus(completionPort.get(), &bytesRead1, &key1, &overlapped1, INFINITE);
    status2 = GetQueuedCompletionStatus(completionPort.get(), &bytesRead2, &key2, &overlapped2, INFINITE);
    assert(key1 == 0);
    assert(key2 == 1);
    if (status1 && status2)
    {
      totalBytesRead += bytesRead1 + bytesRead2;
      gStats.totalBytesRead += bytesRead1 + bytesRead2;

      // Process the completed operations
      if (bytesRead1 == bytesRead2)
      {
        overlapped1->Offset += bytesRead1;
        overlapped2->Offset += bytesRead2;

        compareResult = std::memcmp(ioData[0]->buffer, ioData[1]->buffer, bytesRead1);
        if (compareResult != 0)
        {
          break;
        }
      }
      else
      {
        compareResult = bytesRead1 < bytesRead2;
        break;
      }
    }
    else
    {
      bool success = false;
      success = GetOverlappedResult(ioData[0]->fileHandle.get(), overlapped1, &bytesRead1, FALSE);

      if (!success)
      {
        if (GetLastError() != ERROR_HANDLE_EOF)
        {
          std::cerr << "Error completing asynchronous read." << std::endl;
          gError = 1;
        }
        break;
      }
    }
  } while (bytesRead1 == bytesRead2 && bytesRead1 > 0 && compareResult == 0);

  gError = 0;
  return totalBytesRead;
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

  // Buffer to store the current directory
  TCHAR curDir[MAX_PATH];

  // Get the current working directory
  DWORD len = GetCurrentDirectory(MAX_PATH, curDir);

  std::wstring dir = curDir;
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

  // Attempt to change the current working directory
  if (!SetCurrentDirectory(dir.c_str()))
  {
    std::cerr << "Error changing current working directory. Error code: " << GetLastError() << std::endl;
    gError = 1;
    return result;
  }

  if (len == 0)
  {
    // An error occurred
    std::cerr << "Error getting current directory. Error code: " << GetLastError() << std::endl;
    gError = 1;
    return result;
  }

  // Display the current working directory
  std::wcout << L"Directory: " << dir << std::endl;

  FileInfoMap map;
  GetFileInfoRecursive(dir, map, L"");

  /*
  auto range = map.equal_range("strawberry");
  for_each(
    range.first,
    range.second,
    [](stringmap::value_type& x) {std::cout << " " << x.second; }
  );
  */
  for (auto& entry : map)
  {
    FileInfo& fi = entry.second;
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

    FileHasher fileHasher;

    size_t totalBytesRead = readFile(fi.name, fileHasher);
    assert(totalBytesRead == fi.size);
    fi.contentHash = fileHasher.getHash();
    fi.hashed = true;
    
  }

  for (size_t i = 0; i < map.bucket_count(); ++i)
  {
    for (auto it = map.cbegin(i); it != map.cend(i); ++it)
    {
      const FileInfo& fi = it->second;
      //std::wcout << fi.name << L" size " << fi.size << L" hash " << fi.contentHash << std::endl;
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

  // Compare content of files with the same size/hash (if there is more than 1 file)
  for (auto& it : fileHashMap)
  {
    SizeHashEntry& e = it.second;
    if (e.files.size() > 1)
    {
      findDupContent(e.files, e.multiSet);
    }
  }

  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;

  // Fill out result
  for (auto& it : fileHashMap)
  {
    SizeHashEntry& e = it.second;
    if (e.files.size() > 0)
    {
      if (e.files.size() > 1)
      {
        // Dup content file indices are stored in the set
        AsyncSetIterator itSet = e.multiSet.getIterator();
        while (itSet.hasNext())
        {
          std::vector<std::string> dups;
          std::vector<int> indices = itSet.next();
          for(int i = 0; i < indices.size(); ++i)
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

  return result;
}
