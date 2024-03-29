#include "AsyncFileHasher.h"

#include <cassert>

AsyncFileHasher::AsyncFileHasher(uint32_t blockSize, uint32_t concurrentFilesLimit, ThreadPool& threadPool, IOPool& ioPool) :
  mBlockSize(blockSize),
  mConcurrentFilesLimit(concurrentFilesLimit),
  mThreadPool(threadPool),
  mIOPool(ioPool),
  mMemBlockPool(blockSize, concurrentFilesLimit * 2) // Allocate twice more buffers to not starve
{
  assert(mConcurrentFilesLimit > 0); // 1 blocks is minimum
  mHashFunction = MurmurHash64A; // default
}

void AsyncFileHasher::setHashFunction(HashFunction* fn)
{
  mHashFunction = fn;
}

void AsyncFileHasher::enqueue(FileInfo* fi)
{
  mFiles.push_back(fi);
}

void AsyncFileHasher::calcHashes()
{
  mProcessedFiles = 0;

  for (auto& fi : mFiles)
  {
    IOJob job;
    job.filename = fi->name;
    job.buffer = mMemBlockPool.acquireMemBlock(OWNER_STAGE_SUBMIT, mMemBlockPool.getBlockCount() / 2); // Limit buffer allocation so workers have ones
    job.bufferSize = mBlockSize;
    job.blockReadCallback = sReadBlockCallback;
    job.finishCallback = sReadFinishCallback;
    uint32_t blocksCount = static_cast<uint32_t>(fi->size / mBlockSize + (fi->size % mBlockSize != 0));
    BlockChain* bc = new BlockChain;
    bc->hashBlocks.resize(blocksCount);
    bc->blocksNotReady = blocksCount;
    job.ctx = new Context(this, fi, job.buffer, job.bufferSize, 0, bc);

    mIOPool.submitJob(job);

    int progress = static_cast<int>(100.0f * mProcessedFiles / mFiles.size());
    printf("\rProgress %d%%", progress);
  }
  printf("\r");

  // All files were sent to hash calculation, wait for results
  while (mProcessedFiles < mFiles.size())
  {
    mIOPool.waitWorkers();
    mThreadPool.waitWorkers();
  }
}

IOBuffer AsyncFileHasher::sReadBlockCallback(const uint8_t* block, const uint64_t bytesRead, void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  return context->hasher->readBlockCallback(block, static_cast<uint32_t>(bytesRead), context);
}

IOBuffer AsyncFileHasher::readBlockCallback(const uint8_t* block, const uint32_t bytesRead, Context* ctx)
{
  //LOG("->AsyncFileHasher::readBlockCallback: block 0x%p\n", block);
  assert(ctx);
  assert(bytesRead <= mBlockSize);

  // Create a separate context for next job because of different buffer, pass the current data block
  Context* calcBlockHashCtx = new Context(this, ctx->fileInfo, block, bytesRead, ctx->readOffset, ctx->blockChain);
  mThreadPool.submitWork(sCalcBlockHashCallback, calcBlockHashCtx);

  // Keep track of read position to be able to determine hash block.
  ctx->readOffset += bytesRead;

  // Supply IO with a new buffer to read in
  IOBuffer status;
  status.buffer = mMemBlockPool.acquireMemBlock(OWNER_STAGE_READ, 0);
  status.bufferSize = mBlockSize;
  ctx->block = status.buffer; // Keep the new one to be released in the end.
  //LOG("<-AsyncFileHasher::readBlockCallback\n");
  return status;
}

void AsyncFileHasher::sReadFinishCallback(void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  context->hasher->readFinishCallback(context);
}

void AsyncFileHasher::readFinishCallback(Context* ctx)
{
  //LOG("->AsyncFileHasher::readFinishCallback\n");
  assert(ctx);

  // Memory block isn't needed anymore for file reading
  mMemBlockPool.releaseMemBlock(ctx->block);
  delete ctx;
  //LOG("<-AsyncFileHasher::readFinishCallback\n");
}

void AsyncFileHasher::sCalcBlockHashCallback(void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  context->hasher->calcBlockHashCallback(context);
}

void AsyncFileHasher::calcBlockHashCallback(Context* ctx)
{
  //LOG("->AsyncFileHasher::calcBlockHashCallback\n");
  assert(ctx);

  size_t blockIdx = ctx->readOffset / mBlockSize;
  assert(blockIdx < ctx->blockChain->hashBlocks.size());
  ctx->blockChain->hashBlocks[blockIdx] = mHashFunction(ctx->block, static_cast<int>(ctx->size), 1234);

  std::atomic<uint32_t>& blocksNotReady = ctx->blockChain->blocksNotReady;

  // Memory block isn't needed anymore for hash calc
  mMemBlockPool.releaseMemBlock(ctx->block);
  ctx->block = nullptr;
  ctx->size = 0;

  // Check if it was the last block
  if (blocksNotReady.fetch_sub(1) == 1)
  {
    // The last block was calculated, it's time to reduce the results.
    mThreadPool.submitWork(sCalcFileHashCallback, ctx);
  }
  //LOG("<-AsyncFileHasher::calcBlockHashCallback\n");
}

void AsyncFileHasher::sCalcFileHashCallback(void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  context->hasher->calcFileHashCallback(context);
}

void AsyncFileHasher::calcFileHashCallback(Context* ctx)
{
  //LOG("->AsyncFileHasher::calcFileHashCallback\n");
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
  //LOG("<-AsyncFileHasher::calcFileHashCallback\n");
}
