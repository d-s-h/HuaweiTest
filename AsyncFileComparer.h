#pragma once

#include <mutex>
#include <map>

#include "Platform.h"
#include "ThreadPool.h"
#include "IOPool.h"

class AsyncFileComparer
{
public:
  using FilePair = std::pair<const FileInfo*, const FileInfo*>;
  using Result = std::pair<FilePair, int>;

  AsyncFileComparer(uint32_t blockSize, uint32_t bufferBlockCount, ThreadPool* threadPool, IOPool* ioPool);
  bool enqueue(const FileInfo* fi1, const FileInfo* fi2);
  bool getResults(std::vector<Result>& results);

private:

  struct CompareRequest
  {
    const FileInfo* files[2] = { nullptr, nullptr };
    uint32_t ioId = 0;
    const uint8_t* compareBlocks[2] = { nullptr, nullptr };
    uint32_t compareBlockSizes[2] = { 0, 0 };
    std::atomic<int> blocksLeft;
    int result = 0;
  };

  struct Context
  {
    AsyncFileComparer* fileComparer = nullptr;
    uint32_t fileIdx = 0;
    CompareRequest* req = nullptr;
  };

  static IOStatus sBlockReadCallback(const uint8_t* block, const uint64_t bytesRead, void* ctx);
  static void sReadFinishCallback(void* ctx);
  static void sCompareBlocksWork(void* ctx);

  IOStatus blockReadCallback(const uint8_t* block, const uint32_t bytesRead, Context* ctx);
  void readFinishCallback(Context* ctx);
  void compareBlocksWork(Context* ctx);

  uint8_t* acquireMemBlock(uint8_t ownerId, int limit);
  void releaseMemBlock(const uint8_t* block);

  int mOutstandingRequests;
  std::map<FilePair, int> mCompareResultMap;
  std::vector<Result> mResults;
  uint32_t mBlockSize = 0;
  uint32_t mBufferBlockCount = 0;
  ThreadPool* mThreadPool = nullptr;
  IOPool* mIOPool = nullptr;
  std::mutex mMutex;
  std::condition_variable mCondition;
  std::vector<uint8_t> mFreeBufferBlocks;
  std::vector<uint8_t> mBuffer;
};