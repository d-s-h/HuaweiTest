#pragma once

#include <mutex>

#include "Hash.h"
#include "IOPool.h"
#include "Platform.h"
#include "ThreadPool.h"
#include "MemBlockPool.h"

class FileHasher
{
public:
  FileHasher(uint32_t blockSize, uint32_t bufferBlockCount, ThreadPool& threadPool, IOPool& ioPool);

  void setHashFunction(HashFunction* fn);
  void enqueue(FileInfo* fi);
  void calcHashes();

private:
  // Debug purpose
  static constexpr uint8_t OWNER_STAGE_SUBMIT = 1;
  static constexpr uint8_t OWNER_STAGE_READ = 2;

  using HashBlocks = std::vector<uint64_t>;
  struct BlockChain
  {
    HashBlocks hashBlocks;
    std::atomic<uint32_t> blocksNotReady;
  };

  struct Context
  {
    FileHasher* hasher = nullptr;
    FileInfo* fileInfo = nullptr;
    const uint8_t* block = nullptr;
    uint32_t size = 0;
    uint64_t readOffset = 0;
    BlockChain* blockChain = nullptr;
  };

  static IOStatus sReadBlockCallback(const uint8_t* block, const uint64_t bytesRead, void* ctx);
  static void sReadFinishCallback(void* ctx);
  static void sCalcBlockHashCallback(void* ctx);
  static void sCalcFileHashCallback(void* ctx);

  IOStatus readBlockCallback(const uint8_t* block, const uint32_t bytesRead, Context* ctx);
  void readFinishCallback(Context* ctx);
  void calcBlockHashCallback(Context* ctx);
  void calcFileHashCallback(Context* ctx);

  uint32_t mBlockSize = 0;
  uint32_t mConcurrentFilesLimit = 0;
  ThreadPool& mThreadPool;
  IOPool& mIOPool;
  MemBlockPool mMemBlockPool;
  std::vector<FileInfo*> mFiles;
  std::atomic<int> mProcessedFiles;
  HashFunction* mHashFunction = nullptr;

  std::mutex mMutex;
  std::condition_variable mCondition;
};