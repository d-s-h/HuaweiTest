#pragma once
#include <memory>
#include <string>
#include <unordered_map>

using WorkCallbackFn = void(void* ctx);

struct FileInfo
{
	uint64_t size = 0;
	uint64_t contentHash = 0;
	bool hashed = false;
	std::wstring name;
};
using FileInfoMap = std::unordered_multimap<uint64_t, FileInfo>;

class ThreadPool
{
public:
	ThreadPool(int threadCount);
	~ThreadPool();

	bool submitWork(WorkCallbackFn* cb, void* ctx);
	void waitWorkers();
private:
	std::unique_ptr<class ThreadPoolImpl> mImpl;
};

using BlockCallbackFn = void(const uint8_t* buffer, const uint64_t bytesRead, void* ctx);
using FinishCallbackFn = void(void* ctx);

class IOPool
{
public:
	IOPool(int concurrentIoCount);
	~IOPool();

	bool submitWork(const std::wstring& file, BlockCallbackFn* blockCb, FinishCallbackFn finishCb, void* ctx);
	void waitWorkers();
private:
	std::unique_ptr<class IOPoolImpl> mImpl;
};

class IBlockReadCallback
{
public:
	~IBlockReadCallback() = default;

	virtual void operator()(const uint8_t* block, const size_t size) = 0;
};

bool getCurrentDir(std::wstring& dir);
bool setCurrentDir(const std::wstring& dir);
void getFileInfoRecursive(const std::wstring& directoryPath, FileInfoMap& fileInfoMap, const std::wstring& relativePath);
size_t readFile(const std::wstring& path, IBlockReadCallback& cb);
size_t compareFiles(const std::wstring& path1, const std::wstring& path2, int& compareResult);