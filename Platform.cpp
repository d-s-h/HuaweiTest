#include "Platform.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <tchar.h>
#include <cassert>
#include <iostream>

int gError = 0; // I could use exceptions or change func API to pass an error code but use the global for simplicity.

struct Stats
{
  uint64_t totalBytesRead = 0;
};

Stats gStats;

// Define the size of the buffer for reading from the file
constexpr DWORD BUFFER_SIZE = 4 * 1024 * 1024;

// Custom deleter for HANDLE resources
struct HandleDeleter
{
  void operator()(HANDLE h) const
  {
    CloseHandle(h);
  }
};

using UniqueHandle = std::unique_ptr<void, HandleDeleter>;

// Structure to hold information about an asynchronous file operation
struct FileIOData
{
  FileIOData() : fileHandle(nullptr, HandleDeleter()) {}

  OVERLAPPED overlapped;
  UniqueHandle fileHandle;
  CHAR buffer[BUFFER_SIZE];
};

struct WorkItem
{
  WorkCallbackFn* cb;
  void* ctx;
};

class ThreadPoolImpl
{
public:
  ThreadPoolImpl();
  ~ThreadPoolImpl();

  bool submitWork(WorkCallbackFn* cb, void* ctx);
  void waitWorks();
private:
  uint32_t allocId() { return idAlloc++; }

  PTP_POOL pool = NULL;
  TP_CALLBACK_ENVIRON CallBackEnviron;
  PTP_CLEANUP_GROUP cleanupgroup = NULL;
  std::unordered_map<uint32_t, WorkItem> mWorks;
  uint32_t idAlloc = 0;
};

ThreadPoolImpl::ThreadPoolImpl()
{
  BOOL bRet = FALSE;
  UINT rollback = 0;

  InitializeThreadpoolEnvironment(&CallBackEnviron);

  //
  // Create a custom, dedicated thread pool.
  //
  pool = CreateThreadpool(NULL);

  if (NULL == pool) {
    _tprintf(_T("CreateThreadpool failed. LastError: %u\n"),
      GetLastError());
    goto main_cleanup;
  }

  rollback = 1; // pool creation succeeded

  //
  // The thread pool is made persistent simply by setting
  // both the minimum and maximum threads to 1.
  //
  SetThreadpoolThreadMaximum(pool, 1);

  bRet = SetThreadpoolThreadMinimum(pool, 1);

  if (FALSE == bRet) {
    _tprintf(_T("SetThreadpoolThreadMinimum failed. LastError: %u\n"),
      GetLastError());
    goto main_cleanup;
  }

  //
  // Create a cleanup group for this thread pool.
  //
  cleanupgroup = CreateThreadpoolCleanupGroup();

  if (NULL == cleanupgroup) {
    _tprintf(_T("CreateThreadpoolCleanupGroup failed. LastError: %u\n"),
      GetLastError());
    goto main_cleanup;
  }

  rollback = 2;  // Cleanup group creation succeeded

  //
  // Associate the callback environment with our thread pool.
  //
  SetThreadpoolCallbackPool(&CallBackEnviron, pool);

  //
  // Associate the cleanup group with our thread pool.
  // Objects created with the same callback environment
  // as the cleanup group become members of the cleanup group.
  //
  SetThreadpoolCallbackCleanupGroup(&CallBackEnviron,
    cleanupgroup,
    NULL);

  return;

main_cleanup:
  //
  // Clean up any individual pieces manually
  // Notice the fall-through structure of the switch.
  // Clean up in reverse order.
  //

  switch (rollback) {
  case 4:
  case 3:
    // Clean up the cleanup group members.
    CloseThreadpoolCleanupGroupMembers(cleanupgroup,
      FALSE, NULL);
  case 2:
    // Clean up the cleanup group.
    CloseThreadpoolCleanupGroup(cleanupgroup);

  case 1:
    // Clean up the pool.
    CloseThreadpool(pool);

  default:
    break;
  }
}

ThreadPoolImpl::~ThreadPoolImpl()
{
  //
  // Wait for all callbacks to finish.
  // CloseThreadpoolCleanupGroupMembers also releases objects
  // that are members of the cleanup group, so it is not necessary 
  // to call close functions on individual objects 
  // after calling CloseThreadpoolCleanupGroupMembers.
  //
  CloseThreadpoolCleanupGroupMembers(cleanupgroup, FALSE, NULL);
  CloseThreadpoolCleanupGroup(cleanupgroup);
  CloseThreadpool(pool);
}


//
// This is the thread pool work callback function.
//
VOID CALLBACK WorkCallback(
  PTP_CALLBACK_INSTANCE Instance,
  PVOID                 Parameter,
  PTP_WORK              Work
)
{
  // Instance, Parameter, and Work not used in this example.
  UNREFERENCED_PARAMETER(Instance);
  UNREFERENCED_PARAMETER(Work);

  WorkItem* wi = static_cast<WorkItem*>(Parameter);
  assert(wi);
  wi->cb(wi->ctx);

  // Cleanup allocated work item
  delete wi;

  return;
}

bool ThreadPoolImpl::submitWork(WorkCallbackFn* cb, void* ctx)
{
  WorkItem* wi = new WorkItem;
  PTP_WORK work = CreateThreadpoolWork(WorkCallback, wi, &CallBackEnviron);

  if (NULL == work) {
    printf(("CreateThreadpoolWork failed. LastError: %u\n"), GetLastError());
    return false;
  }
  
  wi->ctx = ctx;
  wi->cb = cb;

  SubmitThreadpoolWork(work);

  return true;  
}

void ThreadPoolImpl::waitWorks()
{
  CloseThreadpoolCleanupGroupMembers(cleanupgroup, FALSE, NULL);
}

ThreadPool::ThreadPool()
{
  mImpl.reset(new ThreadPoolImpl);
}

ThreadPool::~ThreadPool()
{
}

bool ThreadPool::submitWork(WorkCallbackFn* cb, void* ctx)
{
  return mImpl->submitWork(cb, ctx);
}

void ThreadPool::waitWorkers()
{
  mImpl->waitWorks();
}

// Function to fill info for files in a directory recursively
void getFileInfoRecursive(const std::wstring& directoryPath, FileInfoMap& fileInfoMap, const std::wstring& relativePath) {
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
      std::wstring newRelPath = relativePath + (!relativePath.empty() ? L"\\" : L"") + findFileData.cFileName;
      getFileInfoRecursive(subdirectoryPath, fileInfoMap, newRelPath);
    }
    else {
      ULARGE_INTEGER fileSize;
      fileSize.LowPart = findFileData.nFileSizeLow;
      fileSize.HighPart = findFileData.nFileSizeHigh;

      FileInfo fi;
      if (!relativePath.empty())
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

bool getCurrentDir(std::wstring& dir)
{
  // Buffer to store the current directory
  TCHAR curDir[MAX_PATH];

  // Get the current working directory
  DWORD len = GetCurrentDirectory(MAX_PATH, curDir);

  if (len == 0)
  {
    // An error occurred
    std::cerr << "Error getting current directory. Error code: " << GetLastError() << std::endl;
    gError = 1;
    return false;
  }

  dir = curDir;
  return true;
}

bool setCurrentDir(const std::wstring& dir)
{
  // Attempt to change the current working directory
  if (!SetCurrentDirectory(dir.c_str()))
  {
    std::cerr << "Error changing current working directory. Error code: " << GetLastError() << std::endl;
    gError = 1;
    return false;
  }

  return true;
}

size_t readFile(const std::wstring& path, IBlockReadCallback& cb)
{
  //static MemBlockAllocator sBlockAllocator(FILE_BLOCK_SIZE, MAX_CONCURRENT_IO);
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

  for (int i = 0; i < ioData.size(); ++i)
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
    for (int i = 0; i < ioData.size(); ++i)
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
    //assert(key1 == 0);
    //assert(key2 == 1);
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