#include "IOPool.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <vector>
#include <cassert>
#include <iostream>
#include <thread>
#include <mutex>

#include "Platform.h"

struct Stats
{
  std::atomic<uint64_t> totalBytesRead = 0;
};

Stats gStats;

// Define the size of the buffer for reading from the file
constexpr DWORD BUFFER_SIZE = 4 * 1024 * 1024;

// Custom deleter for HANDLE resources
struct HandleDeleter
{
  void operator()(HANDLE h) const
  {
    CloseHandle(h);
  }
};

using UniqueHandle = std::unique_ptr<void, HandleDeleter>;

// Structure to hold information about an asynchronous file operation
struct FileIOData
{
  FileIOData() : fileHandle(nullptr, HandleDeleter()) {}

  OVERLAPPED overlapped;
  UniqueHandle fileHandle;
  IOJob job;
};

class IOPoolImpl
{
public:
  IOPoolImpl(int concurrentIoCount);
  ~IOPoolImpl();

  bool submitJob(IOJob& job);
  void waitWorkers();

  int jobsQueued();

private:
  bool kickOffJob(const IOJob& job, int ioDataIdx);
  void ioDispatcherThread();
  void stop();
  UniqueHandle mCompletionPort;
  std::vector<IOJob> mJobQueue;

  std::thread mWorkerThread;
  std::mutex mMutex;
  std::condition_variable mCondition;
  std::condition_variable mAllJobsDoneCondition;
  bool mStopRequested = false;
  std::vector<FileIOData> mIoData;
  int mConcurrentIoCount = 0;
  uint32_t mFreeIoSlotsMask = 0;
};

IOPool::IOPool(int concurrentIoCount)
{
  mImpl.reset(new IOPoolImpl(concurrentIoCount));
}

IOPool::~IOPool()
{
}

bool IOPool::submitJob(IOJob& job)
{
  return mImpl->submitJob(job);
}

void IOPool::waitWorkers()
{
  mImpl->waitWorkers();
}

int IOPool::jobsQueued()
{
  return mImpl->jobsQueued();
}

IOPoolImpl::IOPoolImpl(int concurrentIoCount)
{
  mConcurrentIoCount = concurrentIoCount;
  // Create an I/O Completion Port
  int threadCount = concurrentIoCount;
  mCompletionPort.reset(CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, threadCount));
  if (mCompletionPort.get() == nullptr)
  {
    std::cerr << "Error creating I/O Completion Port." << std::endl;
    assert(0);
  }

  mIoData.resize(concurrentIoCount);
  mFreeIoSlotsMask = (1 << concurrentIoCount) - 1;
  mWorkerThread = std::thread(&IOPoolImpl::ioDispatcherThread, this);
}

IOPoolImpl::~IOPoolImpl()
{
  stop();
}

bool IOPoolImpl::submitJob(IOJob& job)
{
  std::lock_guard<std::mutex> lock(mMutex);
  WLOG(L"->IOPoolImpl::submitWork: %s, mutex acquired \n", job.filename.c_str());

  mJobQueue.push_back(job);

  // Notify the worker thread that work is available
  mCondition.notify_one();
  WLOG(L"<-IOPoolImpl::submitWork\n");
  return false;
}

int IOPoolImpl::jobsQueued()
{
  // Not very accurate because doesn't count jobs already in progress
  std::lock_guard<std::mutex> lock(mMutex);
  return static_cast<int>(mJobQueue.size());
}

void IOPoolImpl::waitWorkers()
{
  std::unique_lock<std::mutex> lock(mMutex);
  WLOG(L"->IOPoolImpl::waitWorkers\n");
  if (!mJobQueue.empty())
  {
    mAllJobsDoneCondition.wait(lock);
  }
  WLOG(L"<-IOPoolImpl::waitWorkers\n");
}

void IOPoolImpl::stop()
{
  mCompletionPort.reset(); // Close the port, it will force to isDispatcher thread to exit.

  {
    // Lock the mutex to safely modify the stopRequested flag
    std::lock_guard<std::mutex> lock(mMutex);
    mStopRequested = true;
  }

  // Notify the worker thread to stop
  mCondition.notify_one();

  // Wait for the worker thread to join
  if (mWorkerThread.joinable())
  {
    mWorkerThread.join();
  }
}

bool IOPoolImpl::kickOffJob(const IOJob& job, int ioDataIdx)
{
  WLOG(L"->IOPoolImpl::kickOffJob: %s at slot %d\n", job.filename.c_str(), ioDataIdx);
  FileIOData& ioData = mIoData[ioDataIdx];

  ioData.job = job;

  // Open the file for asynchronous reading
  ioData.fileHandle.reset(CreateFile(
    job.filename.c_str(),
    GENERIC_READ,
    FILE_SHARE_READ,
    nullptr,
    OPEN_EXISTING,
    FILE_FLAG_OVERLAPPED,
    nullptr
  ));

  if (ioData.fileHandle.get() == INVALID_HANDLE_VALUE)
  {
    std::cerr << "Error opening file." << std::endl;
    assert(0);
    return false;
  }

  // Associate the file handle with the completion port
  // NumberOfConcurrentThreads parameter is ignored if the ExistingCompletionPort parameter is not NULL.
  if (CreateIoCompletionPort(ioData.fileHandle.get(), mCompletionPort.get(), ioDataIdx, 0) == nullptr)
  {
    std::cerr << "Error associating file handle with I/O Completion Port." << std::endl;
    assert(0);
    return false;
  }

  // Initialize the overlapped structure
  ZeroMemory(&ioData.overlapped, sizeof(OVERLAPPED));

  // Wait for completion of the asynchronous operation
  DWORD bytesRead = 0;
  ULONG_PTR key = NULL;
  LPOVERLAPPED overlapped = NULL;

  // Perform the asynchronous read operation
  if (!ReadFile(ioData.fileHandle.get(), ioData.job.buffer, ioData.job.bufferSize, nullptr, &ioData.overlapped))
  {
    if (GetLastError() != ERROR_IO_PENDING)
    {
      std::cerr << "Error initiating asynchronous read." << std::endl;
      assert(0);
      return false;
    }
  }

  WLOG(L"<-IOPoolImpl::kickOffJob\n");
  return true;
}

int findFirstOne(unsigned int mask)
{
  for (int i = 0; i < sizeof(unsigned int) * 8; ++i) {
    if (((mask >> i) & 1) == 1) {
      return i;
    }
  }
  // If no 0 is found, return -1 (indicating an error or a fully set mask)
  return -1;
}

int countSetBits(unsigned int mask) {
  int count = 0;
  while (mask) {
    mask &= (mask - 1);
    count++;
  }
  return count;
}

void IOPoolImpl::ioDispatcherThread()
{
  while (true)
  {
    std::unique_lock<std::mutex> lock(mMutex);

    // Wait until stop is requested or some work needs to be done
    mCondition.wait(lock, [this] { return mStopRequested || !mJobQueue.empty(); });

    // Check if stop is requested
    if (mStopRequested)
    {
      break;
    }

    WLOG(L"->IOPoolImpl::ioDispatcherThread: there is some work, mutex acquired\n");

    while (!mJobQueue.empty() || mFreeIoSlotsMask != (1 << mConcurrentIoCount) - 1)
    {
      WLOG(L"mJobQueue size: %llu available slots %d\n", mJobQueue.size(), countSetBits(mFreeIoSlotsMask));

      // Kick off the jobs if there are available IO slots.
      while (!mJobQueue.empty() && mFreeIoSlotsMask != 0)
      {
        int freeIdx = findFirstOne(mFreeIoSlotsMask);
        if (freeIdx != -1)
        {
          mFreeIoSlotsMask ^= (1 << freeIdx); // acquire
          bool success = kickOffJob(mJobQueue.front(), freeIdx);
          assert(success);
          mJobQueue.erase(mJobQueue.begin());
        }
      }

      // The job queue is released so can be outside populated with more jobs
      lock.unlock();
      WLOG(L"IOPoolImpl::ioDispatcherThread: all slots occupied, release the mutex\n");

      // Meantime process IO jobs in slots
      DWORD bytesRead = 0;
      ULONG_PTR ioDataIdx = NULL;
      LPOVERLAPPED overlapped = NULL;
      while (GetQueuedCompletionStatus(mCompletionPort.get(), &bytesRead, &ioDataIdx, &overlapped, INFINITE))
      {
        // Process the completed operation
        FileIOData& ioData = mIoData[ioDataIdx];
        IOJob* job = &ioData.job;
        assert(job);
        IOStatus status = job->blockReadCallback(reinterpret_cast<const uint8_t*>(job->buffer), bytesRead, job->ctx);
        if (status.action == IOStatus::Action::CONTINUE)
        {
          if (status.buffer)
          {
            LOG("IOPoolImpl::ioDispatcherThread: new buffer 0x%p\n", status.buffer);
            job->buffer = status.buffer;
            job->bufferSize = status.bufferSize;
          }
        }
        else
        {
          assert(0);
        }
        overlapped->Offset += bytesRead;
        gStats.totalBytesRead += bytesRead;

        // Continue the asynchronous read operation
        if (!ReadFile(ioData.fileHandle.get(), job->buffer, job->bufferSize, nullptr, &ioData.overlapped))
        {
          if (GetLastError() != ERROR_IO_PENDING)
          {
            std::cerr << "Error initiating asynchronous read." << std::endl;
            assert(0);
            return;
          }
        }
      }

      if (overlapped != NULL)
      {
        bool success = false;
        FileIOData& ioData = mIoData[ioDataIdx];
        // Check the result of the asynchronous read without waiting (forth parameter FALSE). 
        success = GetOverlappedResult(ioData.fileHandle.get(), overlapped, &bytesRead, FALSE);

        if (!success)
        {
          if (GetLastError() != ERROR_HANDLE_EOF)
          {
            std::cerr << "Error completing asynchronous read." << std::endl;
            assert(0);
          }
        }

        const IOJob* job = &ioData.job;
        job->finishCallback(job->ctx);
        WLOG(L"Job finished: %s\n", job->filename.c_str());

        // Cleanup and release an IO slot
        ioData.fileHandle = nullptr;
        ioData.job = IOJob();
        ZeroMemory(&ioData.overlapped, sizeof(OVERLAPPED));
        mFreeIoSlotsMask |= (1 << ioDataIdx);
      }
      else
      {
        assert(0);
      }

      // Acquire the job queue again to check for outstanding work.
      lock.lock();
      WLOG(L"IOPoolImpl::ioDispatcherThread: slot is freed, acquire the mutex\n");
    }

    // Clear the workQueue
    mJobQueue.clear();
    mAllJobsDoneCondition.notify_one();
    WLOG(L"<-IOPoolImpl::ioDispatcherThread: main loop\n");
  }


}