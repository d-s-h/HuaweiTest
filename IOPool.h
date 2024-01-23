#pragma once

#include <memory>
#include <string>

struct IOBuffer
{
	uint8_t* buffer = nullptr;
	uint32_t bufferSize = 0;
};

using BlockCallbackFn = IOBuffer(const uint8_t* buffer, const uint64_t bytesRead, void* ctx);
using FinishCallbackFn = void(void* ctx);

struct IOJob
{
	uint32_t jobId = 0; // assigned when submitted
	std::wstring filename;
	uint8_t* buffer = nullptr;
	uint32_t bufferSize = 0;
	BlockCallbackFn* blockReadCallback = nullptr;
	FinishCallbackFn* finishCallback = nullptr;
	void* ctx = nullptr;


};

class IOPool
{
public:
	IOPool(int concurrentIoCount);
	~IOPool();

	int getConcurrentIOCount();
	uint32_t submitJob(IOJob& job);
	void waitWorkers();
	void pause(uint32_t jobId);
	void resume(uint32_t jobId);
	void abort(uint32_t jobId);

	int getJobsQueued();

private:
	std::unique_ptr<class IOPoolImpl> mImpl;
};