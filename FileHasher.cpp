#include "FileHasher.h"

#include <cassert>

FileHasher::FileHasher(uint32_t blockSize, uint32_t bufferBlockCount, ThreadPool* threadPool, IOPool* ioPool) :
  mBlockSize(blockSize), mBufferBlockCount(bufferBlockCount), mThreadPool(threadPool), mIOPool(ioPool)
{
  assert(bufferBlockCount >= 2); // 2 blocks is minimum
  mFreeBufferBlocks.resize(bufferBlockCount, 0);
  mBuffer.resize(bufferBlockCount * blockSize);
  mHashFunction = MurmurHash64A; // default
}

void FileHasher::setHashFunction(HashFunction* fn)
{
  mHashFunction = fn;
}

void FileHasher::enqueue(FileInfo* fi)
{
  mFiles.push_back(fi);
}

void FileHasher::calcHashes()
{
  mProcessedFiles = 0;

  for (auto& fi : mFiles)
  {
    IOJob job;
    job.filename = fi->name;
    job.buffer = acquireReadBlock(OWNER_STAGE_SUBMIT, mBufferBlockCount / 2); // Limit buffer allocation so workers have ones
    job.bufferSize = mBlockSize;
    job.blockReadCallback = sReadBlockCallback;
    job.finishCallback = sReadFinishCallback;
    uint32_t blocksCount = static_cast<uint32_t>(fi->size / mBlockSize + (fi->size % mBlockSize != 0));
    BlockChain* bc = new BlockChain;
    bc->hashBlocks.resize(blocksCount);
    bc->blocksNotReady = blocksCount;
    job.ctx = new Context(this, fi, job.buffer, job.bufferSize, 0, bc);

    mIOPool->submitJob(job);

    int progress = static_cast<int>(100.0f * mProcessedFiles / mFiles.size());
    printf("\rProgress %d%%", progress);
  }
  printf("\r");

  // All files were sent to hash calculation, wait for results
  while (mProcessedFiles < mFiles.size())
  {
    mIOPool->waitWorkers();
    mThreadPool->waitWorkers();
  }
}

IOStatus FileHasher::sReadBlockCallback(const uint8_t* block, const uint64_t bytesRead, void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  return context->hasher->readBlockCallback(block, static_cast<uint32_t>(bytesRead), context);
}

IOStatus FileHasher::readBlockCallback(const uint8_t* block, const uint32_t bytesRead, Context* ctx)
{
  LOG("->FileHasher::readBlockCallback: block 0x%p\n", block);
  assert(ctx);
  assert(bytesRead <= mBlockSize);

  // Create a separate context for next job because of different buffer, pass the current data block
  Context* calcBlockHashCtx = new Context(this, ctx->fileInfo, block, bytesRead, ctx->readOffset, ctx->blockChain);
  mThreadPool->submitWork(sCalcBlockHashCallback, calcBlockHashCtx);

  // Keep track of read position to be able to determine hash block.
  ctx->readOffset += bytesRead;

  // Supply IO with a new buffer to read in
  IOStatus status;
  status.buffer = acquireReadBlock(OWNER_STAGE_READ, 0);
  status.bufferSize = mBlockSize;
  ctx->block = status.buffer; // Keep the new one to be released in the end.
  LOG("<-FileHasher::readBlockCallback\n");
  return status;
}

void FileHasher::sReadFinishCallback(void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  context->hasher->readFinishCallback(context);
  assert(ctx);
}

void FileHasher::readFinishCallback(Context* ctx)
{
  LOG("->FileHasher::readFinishCallback\n");
  assert(ctx);

  // Memory block isn't needed anymore for file reading
  releaseReadBlock(ctx->block);
  delete ctx;
  LOG("<-FileHasher::readFinishCallback\n");
}

void FileHasher::sCalcBlockHashCallback(void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  context->hasher->calcBlockHashCallback(context);
}

void FileHasher::calcBlockHashCallback(Context* ctx)
{
  LOG("->FileHasher::calcBlockHashCallback\n");
  assert(ctx);

  size_t blockIdx = ctx->readOffset / mBlockSize;
  assert(blockIdx < ctx->blockChain->hashBlocks.size());
  ctx->blockChain->hashBlocks[blockIdx] = mHashFunction(ctx->block, static_cast<int>(ctx->size), 1234);

  std::atomic<uint32_t>& blocksNotReady = ctx->blockChain->blocksNotReady;

  // Memory block isn't needed anymore for hash calc
  releaseReadBlock(ctx->block);
  ctx->block = nullptr;
  ctx->size = 0;

  // Check if it was the last block
  if (blocksNotReady.fetch_sub(1) == 1)
  {
    // The last block was calculated, it's time to reduce the results.
    mThreadPool->submitWork(sCalcFileHashCallback, ctx);
  }
  LOG("<-FileHasher::calcBlockHashCallback\n");
}

void FileHasher::sCalcFileHashCallback(void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  context->hasher->calcFileHashCallback(context);
}

void FileHasher::calcFileHashCallback(Context* ctx)
{
  LOG("->FileHasher::calcFileHashCallback\n");
  assert(ctx);
  assert(ctx->blockChain);
  for (const uint64_t blockHash : ctx->blockChain->hashBlocks)
  {
    ctx->fileInfo->contentHash += blockHash;
  }
  ctx->fileInfo->hashed = true;
  ++mProcessedFiles;

  // Release all context resources
  delete ctx->blockChain;
  delete ctx;
  LOG("<-FileHasher::calcFileHashCallback\n");
}

uint8_t* FileHasher::acquireReadBlock(uint8_t ownerId, int limit)
{
  LOG("->FileHasher::acquireReadBlock\n");
  assert(ownerId > 0);
  std::unique_lock<std::mutex> lock(mMutex);

  // Wait until a block is available
  mCondition.wait(lock,
    [this, limit]()
    {
      return std::count(mFreeBufferBlocks.begin(), mFreeBufferBlocks.end(), 0) > limit;
    });

  uint8_t* block = nullptr;
  size_t idx = 0;
  auto it = std::find(mFreeBufferBlocks.begin(), mFreeBufferBlocks.end(), 0);
  if (it != mFreeBufferBlocks.end())
  {
    idx = it - mFreeBufferBlocks.begin();
    assert(mFreeBufferBlocks[idx] == 0);
    mFreeBufferBlocks[idx] = ownerId;
    block = &mBuffer[idx * mBlockSize];
  }
  else
  {
    assert(0);
  }
  LOG("<-FileHasher::acquireReadBlock: idx %lld 0x%p\n", idx, block);
  return block;
}

void FileHasher::releaseReadBlock(const uint8_t* block)
{
  size_t idx = (block - mBuffer.data()) / mBlockSize;
  LOG("->FileHasher::releaseReadBlock: idx %lld 0x%p\n", idx, block);
  assert(idx >= 0 && idx < mFreeBufferBlocks.size());
  std::lock_guard<std::mutex> lock(mMutex);
  assert(mFreeBufferBlocks[idx] > 0);
  mFreeBufferBlocks[idx] = 0;
  mCondition.notify_one(); // notify that a block has been release so it can be acquired again.
  LOG("<-FileHasher::releaseReadBlock\n");
}
