#include "Platform.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <tchar.h>
#include <cassert>
#include <iostream>

void WDebugOut(const wchar_t* fmt, ...)
{
  va_list argp;
  va_start(argp, fmt);
  wchar_t dbg_out[4096];
  vswprintf_s(dbg_out, fmt, argp);
  va_end(argp);
  OutputDebugStringW(dbg_out);
}

void DebugOut(const char* fmt, ...)
{
  va_list argp;
  va_start(argp, fmt);
  char dbg_out[4096];
  vsprintf_s(dbg_out, fmt, argp);
  va_end(argp);
  OutputDebugStringA(dbg_out);
}

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

uint32_t getCurrentThreadId()
{
  return GetCurrentThreadId();
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
    assert(0);
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
    assert(0);
    return false;
  }

  return true;
}
