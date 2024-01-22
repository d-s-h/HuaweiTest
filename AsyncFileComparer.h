#pragma once

#include <mutex>
#include <map>

#include "Platform.h"
#include "ThreadPool.h"
#include "IOPool.h"
#include "MemBlockPool.h"

class AsyncFileComparer
{
public:
  using FilePair = std::pair<const FileInfo*, const FileInfo*>;
  using Result = std::pair<FilePair, int>;

  AsyncFileComparer(uint32_t blockSize, ThreadPool* threadPool, IOPool* ioPool);
  bool enqueue(const FileInfo* fi1, const FileInfo* fi2);
  bool getResults(std::vector<Result>& results);

private:

  struct CompareRequest
  {
    const FileInfo* files[2] = { nullptr, nullptr };
    uint32_t jobIds[2] = { 0, 0 };
    const uint8_t* compareBlocks[2] = { nullptr, nullptr };
    uint32_t compareBlockSizes[2] = { 0, 0 };
    std::atomic<int> blocksToCompare;
    int result = 0;
  };

  struct Context
  {
    AsyncFileComparer* fileComparer = nullptr;
    const uint8_t* memBlock = nullptr;
    uint32_t fileIdx = 0;
    CompareRequest* req = nullptr;
  };

  static IOStatus sBlockReadCallback(const uint8_t* block, const uint64_t bytesRead, void* ctx);
  static void sReadFinishCallback(void* ctx);
  static void sCompareBlocksWork(void* ctx);

  IOStatus blockReadCallback(const uint8_t* block, const uint32_t bytesRead, Context* ctx);
  void readFinishCallback(Context* ctx);
  void compareBlocksWork(Context* ctx);
  void finishResult(const Result& result);

  std::atomic<int> mOutstandingRequests;
  std::map<FilePair, int> mCompareResultMap;
  std::vector<Result> mResults;
  ThreadPool* mThreadPool = nullptr;
  IOPool* mIOPool = nullptr;
  MemBlockPool mMemBlockPool;

  std::mutex mMutex;
  std::condition_variable mCondition;
};