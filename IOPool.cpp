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
  uint64_t totalBytesRead = 0;
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

struct FindSlotByJobIdPred
{
  FindSlotByJobIdPred(int jobId) : jobId(jobId) {}
  bool operator()(const FileIOData& data) const { return data.job.jobId == jobId; }
  int jobId = 0;
};

struct FindSlotByKeyPred
{
  FindSlotByKeyPred(ULONG_PTR key) : key(key) {}
  bool operator()(const FileIOData& data) const { return reinterpret_cast<ULONG_PTR>(data.fileHandle.get()) == key; }
  ULONG_PTR key = 0;
};

static inline int findFirstOne(unsigned int mask)
{
  for (int i = 0; i < sizeof(unsigned int) * 8; ++i) {
    if (((mask >> i) & 1) == 1) {
      return i;
    }
  }
  // If no 0 is found, return -1 (indicating an error or a fully set mask)
  return -1;
}

static inline int countSetBits(unsigned int mask) {
  int count = 0;
  while (mask) {
    mask &= (mask - 1);
    count++;
  }
  return count;
}

class IOPoolImpl
{
public:
  IOPoolImpl(int concurrentIoCount);
  ~IOPoolImpl();

  int getConcurrentIOCount() { return mConcurrentIoCount; }
  uint32_t submitJob(IOJob& job);
  void waitWorkers();
  void pause(uint32_t jobId);
  void resume(uint32_t jobId);
  void abort(uint32_t jobId);

  int getJobsQueued();

private:
  bool kickOffJob(const IOJob& job, int ioDataIdx);
  void ioDispatcherThread();
  bool pendingJobsEmpty();
  int acquireIOSlot();
  void releaseIOSlot(const int ioDataIdx);
  void stop();

  uint32_t mIdAllocator = 0;
  UniqueHandle mCompletionPort;
  std::vector<IOJob> mJobQueue;
  std::vector<FileIOData> mPausedJobs;
  std::vector<FileIOData> mResumedJobs;

  std::thread mDispatcherThread;
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

int IOPool::getConcurrentIOCount()
{
  return mImpl->getConcurrentIOCount();
}

uint32_t IOPool::submitJob(IOJob& job)
{
  return mImpl->submitJob(job);
}

void IOPool::waitWorkers()
{
  mImpl->waitWorkers();
}

void IOPool::pause(uint32_t jobId)
{
  mImpl->pause(jobId);
}

void IOPool::resume(uint32_t jobId)
{
  mImpl->resume(jobId);
}

void IOPool::abort(uint32_t jobId)
{
  mImpl->abort(jobId);
}

int IOPool::getJobsQueued()
{
  return mImpl->getJobsQueued();
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
  mDispatcherThread = std::thread(&IOPoolImpl::ioDispatcherThread, this);
}

IOPoolImpl::~IOPoolImpl()
{
  stop();
}

uint32_t IOPoolImpl::submitJob(IOJob& job)
{
  std::lock_guard<std::mutex> lock(mMutex);

  uint32_t id = ++mIdAllocator;
  WLOG(L"->IOPoolImpl::submitJob: %d\n", id);
  job.jobId = id;
  mJobQueue.push_back(job);

  // Notify the worker thread that work is available
  mCondition.notify_one();
  WLOG(L"<-IOPoolImpl::submitJob\n");
  return id;
}

int IOPoolImpl::getJobsQueued()
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

void IOPoolImpl::pause(uint32_t jobId)
{
  assert(jobId > 0);
  if (std::this_thread::get_id() == mDispatcherThread.get_id())
  {
    auto it = std::find_if(mIoData.begin(), mIoData.end(), FindSlotByJobIdPred(jobId));
    assert(it != mIoData.end());
    size_t ioSlotIdx = it - mIoData.begin();
    {
      std::lock_guard<std::mutex> lock(mMutex);
      FileIOData& ioData = mPausedJobs.emplace_back(std::move(mIoData[ioSlotIdx])); // move all the content
      WLOG(L"Job is paused: %d buf 0x%p\n", ioData.job.jobId, ioData.job.buffer);
    }
    releaseIOSlot(static_cast<int>(ioSlotIdx));
  }
  else
  {
    assert(0); // Only calls from the dispatcher thread are supported
  }
}

void IOPoolImpl::resume(uint32_t jobId)
{
  assert(jobId > 0);
  std::lock_guard<std::mutex> lock(mMutex);
  auto it = std::find_if(mPausedJobs.begin(), mPausedJobs.end(), FindSlotByJobIdPred(jobId));
  if (it != mPausedJobs.end())
  {
    mResumedJobs.emplace_back(std::move(*it));
    mPausedJobs.erase(it);
    mCondition.notify_one();
  }
  else
  {
    assert(0);
  }
}

void IOPoolImpl::abort(uint32_t jobId)
{
  assert(jobId > 0);
  if (std::this_thread::get_id() == mDispatcherThread.get_id())
  {
    // Called from the dispatcher thread, no need to guard access to pending IO
    auto it = std::find_if(mIoData.begin(), mIoData.end(), FindSlotByJobIdPred(jobId));
    assert(it != mIoData.end());
    it->job.finishCallback(it->job.ctx);
    WLOG(L"Job aborted: %d\n", it->job.jobId);
    size_t ioSlotIdx = it - mIoData.begin();
    releaseIOSlot(static_cast<int>(ioSlotIdx));
    mIoData.erase(it);
  }
  else
  {
    // Called outside of dispatcher thread, only paused jobs abort is supported for now
    std::lock_guard<std::mutex> lock(mMutex);
    auto it = std::find_if(mPausedJobs.begin(), mPausedJobs.end(), FindSlotByJobIdPred(jobId));
    assert(it != mPausedJobs.end());
    it->job.finishCallback(it->job.ctx);
    WLOG(L"Job aborted: %d\n", it->job.jobId);
    assert(it != mPausedJobs.end());
    mPausedJobs.erase(it);
  }
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
  if (mDispatcherThread.joinable())
  {
    mDispatcherThread.join();
  }
}

bool IOPoolImpl::kickOffJob(const IOJob& job, int ioDataIdx)
{
  LOG("->IOPoolImpl::kickOffJob: %d at slot %d buf 0x%p\n", job.jobId, ioDataIdx, job.buffer);
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
  ULONG_PTR key = reinterpret_cast<ULONG_PTR>(ioData.fileHandle.get()); // Serve HANDLE as a key
  if (CreateIoCompletionPort(ioData.fileHandle.get(), mCompletionPort.get(), key, 0) == nullptr)
  {
    std::cerr << "Error associating file handle with I/O Completion Port." << std::endl;
    assert(0);
    return false;
  }
  LOG("IOPoolImpl::kickOffJob: key/handle = %llu\n", key);

  // Initialize the overlapped structure
  ZeroMemory(&ioData.overlapped, sizeof(OVERLAPPED));

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

void IOPoolImpl::ioDispatcherThread()
{
  while (true)
  {
    std::unique_lock<std::mutex> lock(mMutex);

    // Wait until stop is requested or some work needs to be done
    mCondition.wait(lock, [this] { return mStopRequested || !mJobQueue.empty() || !mResumedJobs.empty(); });

    // Check if stop is requested
    if (mStopRequested)
    {
      break;
    }

    WLOG(L"->IOPoolImpl::ioDispatcherThread: there is some work, mutex acquired\n");

    while (!mJobQueue.empty() || !mResumedJobs.empty() || !pendingJobsEmpty())
    {
      LOG("mJobQueue(%llu), mResumedJobs(%llu), available slots %d\n", mJobQueue.size(), mResumedJobs.size(), countSetBits(mFreeIoSlotsMask));

      // Continue any resumed jobs if there are available IO slots.
      while (!mResumedJobs.empty() && mFreeIoSlotsMask != 0)
      {
        int idx = acquireIOSlot();
        assert(idx != -1);
        if (idx != -1)
        {
          mIoData[idx] = std::move(mResumedJobs.front());
          FileIOData& ioData = mIoData[idx];
          mResumedJobs.erase(mResumedJobs.begin());
          LOG("->IOPoolImpl::ioDispatcherThread: %d resumed at slot %d buf 0x%p\n", ioData.job.jobId, idx, ioData.job.buffer);
          // Continue the asynchronous read operation
          if (!ReadFile(ioData.fileHandle.get(), ioData.job.buffer, ioData.job.bufferSize, nullptr, &ioData.overlapped))
          {
            if (GetLastError() != ERROR_IO_PENDING)
            {
              std::cerr << "Error initiating asynchronous read." << std::endl;
              assert(0);
              return;
            }
          }
          LOG("Job is resumed: %d\n", mIoData[idx].job.jobId);
        }
      }

      // Kick off jobs if there are available IO slots.
      while (!mJobQueue.empty() && mFreeIoSlotsMask != 0)
      {
        int idx = acquireIOSlot();
        assert(idx != -1);
        if (idx != -1)
        {
          bool success = kickOffJob(mJobQueue.front(), idx);
          assert(success);
          mJobQueue.erase(mJobQueue.begin());
        }
      }

      // The job queue is released so can be outside populated with more jobs
      lock.unlock();
      WLOG(L"IOPoolImpl::ioDispatcherThread: all slots occupied, release the mutex\n");

      // Meantime process IO jobs in slots
      DWORD bytesRead = 0;
      ULONG_PTR key = NULL;
      LPOVERLAPPED overlapped = NULL;
      IOBuffer status;
      size_t ioSlotIdx = 0;
      while (GetQueuedCompletionStatus(mCompletionPort.get(), &bytesRead, &key, &overlapped, INFINITE))
      {
        auto slotBeforeCbIt = std::find_if(mIoData.begin(), mIoData.end(), FindSlotByKeyPred(key));
        assert(slotBeforeCbIt != mIoData.end());
        ioSlotIdx = slotBeforeCbIt - mIoData.begin();
        // Process the completed operation
        FileIOData& ioData = mIoData[ioSlotIdx];
        int jobId = ioData.job.jobId;
        assert(jobId != 0);
        IOJob& job = ioData.job;
        overlapped->Offset += bytesRead;
        gStats.totalBytesRead += bytesRead;
        status = job.blockReadCallback(reinterpret_cast<const uint8_t*>(job.buffer), bytesRead, job.ctx);

        // Check if the job was aborted or paused
        auto itSlotAfterCb = std::find_if(mIoData.begin(), mIoData.end(), FindSlotByKeyPred(key));
        if (itSlotAfterCb == mIoData.end())
        {
          // Check if a new buffer is provided
          if (status.buffer)
          {
            // Store the new buffer in the paused job
            lock.lock();
            if(auto it = std::find_if(mPausedJobs.begin(), mPausedJobs.end(), FindSlotByKeyPred(key)); it != mPausedJobs.end())
            {
              LOG("IOPoolImpl::ioDispatcherThread: paused job %d new buffer 0x%p\n", it->job.jobId, status.buffer);
              it->job.buffer = status.buffer;
              it->job.bufferSize = status.bufferSize;
            }
            else if (auto it = std::find_if(mResumedJobs.begin(), mResumedJobs.end(), FindSlotByKeyPred(key)); it != mResumedJobs.end())
            {
              LOG("IOPoolImpl::ioDispatcherThread: paused and resumed job %d new buffer 0x%p\n", it->job.jobId, status.buffer);
              it->job.buffer = status.buffer;
              it->job.bufferSize = status.bufferSize;
            }
            else
            {
              LOG("IOPoolImpl::ioDispatcherThread: the job %d was aborted\n", jobId);
            }
            lock.unlock();
          }

          overlapped = NULL;
          if (pendingJobsEmpty())
          {
            // It was the last job in the pending list so exit the GetQueuedCompletionStatus loop.
            break;
          }
        }
        else
        {
          // Check if a new buffer is provided
          if (status.buffer)
          {
            LOG("IOPoolImpl::ioDispatcherThread: job %d new buffer 0x%p\n", job.jobId, status.buffer);
            job.buffer = status.buffer;
            job.bufferSize = status.bufferSize;
          }

          // Continue the asynchronous read operation
          if (!ReadFile(ioData.fileHandle.get(), job.buffer, job.bufferSize, nullptr, &ioData.overlapped))
          {
            if (GetLastError() != ERROR_IO_PENDING)
            {
              std::cerr << "Error initiating asynchronous read." << std::endl;
              assert(0);
              return;
            }
          }
        }
      }

      if (overlapped != NULL)
      {
        auto it = std::find_if(mIoData.begin(), mIoData.end(), FindSlotByKeyPred(key));
        assert(it != mIoData.end());
        ioSlotIdx = it - mIoData.begin();
        FileIOData& ioData = mIoData[ioSlotIdx];
        assert(ioData.job.jobId != 0);
        // Check the result of the asynchronous read without waiting (forth parameter FALSE). 
        bool success = false;
        success = GetOverlappedResult(ioData.fileHandle.get(), overlapped, &bytesRead, FALSE);

        if (!success)
        {
          if (GetLastError() != ERROR_HANDLE_EOF)
          {
            std::cerr << "Error completing asynchronous read." << std::endl;
            assert(0);
          }
          LOG("IOPoolImpl::ioDispatcherThread: GetOverlappedResult h=%p\n", ioData.fileHandle.get());
        }

        const IOJob& job = ioData.job;
        job.finishCallback(job.ctx);
        WLOG(L"Job finished: %d\n", job.jobId);

        releaseIOSlot(static_cast<int>(ioSlotIdx));
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

bool IOPoolImpl::pendingJobsEmpty()
{
  return mFreeIoSlotsMask == (1 << mConcurrentIoCount) - 1;
}

int IOPoolImpl::acquireIOSlot()
{
  int idx = findFirstOne(mFreeIoSlotsMask);
  if (idx != -1)
  {
    mFreeIoSlotsMask ^= (1 << idx); // acquire
  }
  return idx;
}

void IOPoolImpl::releaseIOSlot(const int ioDataIdx)
{
  // Allowed to call from the dispatcher thread only.
  assert(std::this_thread::get_id() == mDispatcherThread.get_id());
  // Cleanup and release an IO slot
  FileIOData& ioData = mIoData[ioDataIdx];
  ioData.fileHandle = nullptr;
  ioData.job = IOJob();
  ZeroMemory(&ioData.overlapped, sizeof(OVERLAPPED));
  mFreeIoSlotsMask |= (1 << ioDataIdx);
}
