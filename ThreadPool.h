#pragma once

#include <memory>

using WorkCallbackFn = void(void* ctx);

class ThreadPool
{
public:
	ThreadPool(int threadCount);
	~ThreadPool();

	int getThreadCount();
	bool submitWork(WorkCallbackFn* cb, void* ctx);
	void waitWorkers();
private:
	std::unique_ptr<class ThreadPoolImpl> mImpl;
};