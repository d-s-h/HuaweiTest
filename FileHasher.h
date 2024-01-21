#pragma once

#include <thread>
#include <mutex>

#include "Platform.h"
#include "ThreadPool.h"
#include "IOPool.h"
#include "Hash.h"

class FileHasher
{
public:
  FileHasher(uint32_t blockSize, uint32_t bufferBlockCount, ThreadPool* threadPool, IOPool* ioPool);

  void setHashFunction(HashFunction* fn);
  void enqueue(FileInfo* fi);
  void calcHashes();

private:
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

  uint8_t* acquireReadBlock(uint8_t ownerId, int limit);
  void releaseReadBlock(const uint8_t* block);

  std::mutex mMutex;
  std::condition_variable mCondition;
  std::vector<FileInfo*> mFiles;
  ThreadPool* mThreadPool = nullptr;
  IOPool* mIOPool = nullptr;
  uint32_t mBufferBlockCount = 0;
  uint32_t mBlockSize = 0;
  std::vector<uint8_t> mFreeBufferBlocks;
  std::vector<uint8_t> mBuffer;
  std::atomic<int> mProcessedFiles;
  HashFunction* mHashFunction = nullptr;
};