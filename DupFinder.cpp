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

#include "DupFinder.h"
#include "Hash.h"

int gError = 0; // I could use exceptions or change func API to pass an error code but use the global for simplicity.
HashFunction* gHashFunction = MurmurHash64A;

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
  OVERLAPPED overlapped;
  HANDLE fileHandle;
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

int readFile(const std::wstring& path, IBlockReadCallback& cb)
{
  int result = 0;

  // Open the file for asynchronous reading
  UniqueHandle fileHandle(CreateFile(
    path.c_str(),
    GENERIC_READ,
    FILE_SHARE_READ,
    nullptr,
    OPEN_EXISTING,
    FILE_FLAG_OVERLAPPED,
    nullptr
  ), HandleDeleter());

  if (fileHandle.get() == INVALID_HANDLE_VALUE)
  {
    std::cerr << "Error opening file." << std::endl;
    gError = 1;
    return result;
  }

  // Create an I/O Completion Port
  DWORD threadCount = 0;
  UniqueHandle completionPort(CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, threadCount), HandleDeleter());
  if (completionPort == nullptr)
  {
    std::cerr << "Error creating I/O Completion Port." << std::endl;
    gError = 1;
    return result;
  }

  // Associate the file handle with the completion port
  // NumberOfConcurrentThreads parameter is ignored if the ExistingCompletionPort parameter is not NULL.
  if (CreateIoCompletionPort(fileHandle.get(), completionPort.get(), 0, 0) == nullptr)
  {
    std::cerr << "Error associating file handle with I/O Completion Port." << std::endl;
    gError = 1;
    return result;
  }

  // Create and initialize the data structure for I/O operations
  std::unique_ptr<FileIOData> ioData = std::make_unique<FileIOData>();
  ioData->fileHandle = fileHandle.get();

  // Initialize the overlapped structure
  ZeroMemory(&ioData->overlapped, sizeof(OVERLAPPED));

  // Perform the asynchronous read operation
  if (!ReadFile(fileHandle.get(), ioData->buffer, BUFFER_SIZE, nullptr, &ioData->overlapped))
  {
    if (GetLastError() != ERROR_IO_PENDING)
    {
      std::cerr << "Error initiating asynchronous read." << std::endl;
      gError = 1;
      return result;
    }
  }

  // Wait for completion of the asynchronous operation
  DWORD bytesRead;
  ULONG_PTR key;
  LPOVERLAPPED overlapped;

  if (GetQueuedCompletionStatus(completionPort.get(), &bytesRead, &key, &overlapped, INFINITE))
  {
    // Process the completed operation
    cb(reinterpret_cast<const uint8_t*>(ioData->buffer), bytesRead);
  }
  else
  {
    std::cerr << "Error completing asynchronous read." << std::endl;
  }

  gError = 0;
  return result;
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

    readFile(fi.name, fileHasher);
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

  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
  // Fill out result
  for (auto& it : fileHashMap)
  {
    SizeHashEntry& e = it.second;
    if (e.files.size() > 0)
    {
      std::vector<std::string> dups;
      for (size_t i = 0; i < e.files.size(); ++i)
      {
        const FileInfo* fi = e.files[i];
        std::string narrow = converter.to_bytes(fi->name);
        dups.emplace_back(narrow);

        std::cout << ((i != 0) ? "  " : "") << narrow << " size " << fi->size << " hash " << fi->contentHash << std::endl;
      }
      result.emplace_back(dups);
    }
  }

  return result;
}
