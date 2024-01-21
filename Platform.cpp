#include "Platform.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <tchar.h>
#include <cassert>
#include <iostream>

int gError = 0; // I could use exceptions or change func API to pass an error code but use the global for simplicity.

// Custom deleter for HANDLE resources
struct HandleDeleter
{
  void operator()(HANDLE h) const
  {
    CloseHandle(h);
  }
};

using UniqueHandle = std::unique_ptr<void, HandleDeleter>;



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

// Define the size of the buffer for reading from the file
constexpr DWORD BUFFER_SIZE = 4 * 1024 * 1024;

// Structure to hold information about an asynchronous file operation
struct FileIOData1
{
  FileIOData1() : fileHandle(nullptr, HandleDeleter()) {}

  OVERLAPPED overlapped;
  UniqueHandle fileHandle;
  CHAR buffer[BUFFER_SIZE];
};

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
  std::vector<std::unique_ptr<FileIOData1>> ioData(2);
  const std::wstring* files[2] = { &path1, &path2 };

  for (int i = 0; i < ioData.size(); ++i)
  {
    ioData[i] = std::make_unique<FileIOData1>();

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

