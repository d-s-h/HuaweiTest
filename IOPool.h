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

// IO job description.
// Memory buffers and callbacks are provided by user.
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

// Read files in parallel using an OS specific API
// Uses a pImpl idiom to hide platform specific implementation.
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