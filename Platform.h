#pragma once
#include <string>
#include <unordered_map>

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