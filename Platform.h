#pragma once
#include <string>
#include <unordered_map>

void DebugOut(const char* fmt, ...);
void WDebugOut(const wchar_t* fmt, ...);

#ifdef _DEBUG
#define LOG(format, ...) \
  do { \
		DebugOut(format, ##__VA_ARGS__); \
  } while(false);

#define WLOG(format, ...) \
  do { \
		WDebugOut(format, ##__VA_ARGS__); \
  } while(false);
#else
#define LOG(format, ...)
#define WLOG(format, ...)
#endif

struct FileInfo
{
	uint64_t size = 0;
	uint64_t contentHash = 0;
	bool hashed = false;
	std::wstring name;
};
using FileInfoMap = std::unordered_multimap<uint64_t, FileInfo>;

bool getCurrentDir(std::wstring& dir);
bool setCurrentDir(const std::wstring& dir);
void getFileInfoRecursive(const std::wstring& directoryPath, FileInfoMap& fileInfoMap, const std::wstring& relativePath);
size_t compareFiles(const std::wstring& path1, const std::wstring& path2, int& compareResult);
uint32_t getCurrentThreadId();