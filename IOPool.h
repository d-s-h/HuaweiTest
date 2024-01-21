#pragma once

#include <memory>
#include <string>

using BlockCallbackFn = void(const uint8_t* buffer, const uint64_t bytesRead, void* ctx);
using FinishCallbackFn = void(void* ctx);

class IOPool
{
public:
	IOPool(int concurrentIoCount);
	~IOPool();

	bool submitWork(const std::wstring& file, BlockCallbackFn* blockCb, FinishCallbackFn finishCb, void* ctx);
	void waitWorkers();

	int jobsQueued();
private:
	std::unique_ptr<class IOPoolImpl> mImpl;
};

class IBlockReadCallback
{
public:
	~IBlockReadCallback() = default;

	virtual void operator()(const uint8_t* block, const size_t size) = 0;
};
