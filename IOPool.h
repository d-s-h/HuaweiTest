#pragma once

#include <memory>
#include <string>

struct IOStatus
{
	enum class Action
	{
		CONTINUE,
		ABORT
	};
	Action action = Action::CONTINUE;
	uint8_t* buffer = nullptr;
	uint32_t bufferSize = 0;
};

using BlockCallbackFn = IOStatus(const uint8_t* buffer, const uint64_t bytesRead, void* ctx);
using FinishCallbackFn = void(void* ctx);

struct IOJob
{
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

	bool submitJob(IOJob& job);
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
