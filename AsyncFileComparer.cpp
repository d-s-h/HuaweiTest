#include "AsyncFileComparer.h"

#include <cassert>

AsyncFileComparer::AsyncFileComparer(uint32_t blockSize, int concurrentFilesLimit, ThreadPool& threadPool, IOPool& ioPool) :
  mConcurrentFilesLimit(concurrentFilesLimit),
  mThreadPool(threadPool),
  mIOPool(ioPool),
  mMemBlockPool(blockSize, mConcurrentFilesLimit * 2) // Allocate twice more to avoid starving
{
}

bool AsyncFileComparer::enqueue(const FileInfo* fi1, const FileInfo* fi2)
{
  // Allocate resources for a job
  CompareRequest* req = new CompareRequest;
  req->files[0] = fi1;
  req->files[1] = fi2;
  assert(req->files[0]->size == req->files[1]->size);
  assert(fi1 != fi2);
  // Total blocks to compare for two files
  uint64_t size = req->files[0]->size;
  int rem = size % mMemBlockPool.getBlockSize() > 0;
  uint64_t totalBlocks = 2 * (size / mMemBlockPool.getBlockSize() + rem);
  req->blocksToCompare = static_cast<int>(totalBlocks);

  const FileInfo* files[2] = { fi1, fi2 };

  int submitJobsBlockLimit = mMemBlockPool.getBlockCount() / 2;
  IOJob jobs[2];
  for (uint32_t i = 0; i < 2; ++i)
  {
    jobs[i].filename = files[i]->name;
    jobs[i].buffer = mMemBlockPool.acquireMemBlock(UINT_MAX, submitJobsBlockLimit);
    jobs[i].bufferSize = mMemBlockPool.getBlockSize();
    jobs[i].blockReadCallback = sBlockReadCallback;
    jobs[i].finishCallback = sReadFinishCallback;
    jobs[i].ctx = new Context({ this, jobs[i].buffer, i, req });

    int jobId = mIOPool.submitJob(jobs[i]);
    req->jobIds[i] = jobId; // Store job ids to be able to resume or abort reading ops.
  }
  WLOG(L"Job submitted: %s <> %s -> (%d, %d), buffers (0x%p, 0x%p)\n",
    fi1->name.c_str(), fi2->name.c_str(), jobs[0].jobId, jobs[1].jobId, jobs[0].buffer, jobs[1].buffer);

  ++mOutstandingRequests;
  return true;
}

bool AsyncFileComparer::getResults(std::vector<Result>& results)
{
  std::unique_lock<std::mutex> lock(mMutex);

  if (mOutstandingRequests == 0)
  {
    // Nothing to wait
    return false;
  }

  // Wait until results are available
  mCondition.wait(lock, [this]() { return !mResults.empty(); });

  results = std::move(mResults); // just move the content of the container.
  mResults.clear();

  return true;
}

IOStatus AsyncFileComparer::sBlockReadCallback(const uint8_t* block, const uint64_t bytesRead, void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  assert(context->fileComparer);
  return context->fileComparer->blockReadCallback(block, static_cast<uint32_t>(bytesRead), context);
}

void AsyncFileComparer::sReadFinishCallback(void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  assert(context->fileComparer);
  return context->fileComparer->readFinishCallback(context);
}

void AsyncFileComparer::sCompareBlocksWork(void* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  assert(context->fileComparer);
  return context->fileComparer->compareBlocksWork(context);
}

IOStatus AsyncFileComparer::blockReadCallback(const uint8_t* block, const uint32_t bytesRead, Context* ctx)
{
  uint32_t fileIdx = ctx->fileIdx;
  CompareRequest* req = ctx->req;
  LOG("(%u) ->AsyncFileComparer::blockReadCallback: job %d 0x%p\n", getCurrentThreadId(), req->jobIds[fileIdx], block);
  uint32_t blockOwner = mMemBlockPool.getOwnerId(block);
  assert(blockOwner == UINT_MAX || blockOwner == req->jobIds[fileIdx]);

  // Allocate new buffer before pausing because it should be correctly cleaned up if aborted during pause
  IOStatus status;
  status.buffer = mMemBlockPool.acquireMemBlock(req->jobIds[fileIdx], 0); // allocate new memory for reading
  status.bufferSize = mMemBlockPool.getBlockSize();
  ctx->memBlock = status.buffer; // Store to release it when reading is finished

  // Send IO to a pending state until blocks are compared
  mIOPool.pause(req->jobIds[fileIdx]);

  req->compareBlocks[fileIdx] = block;  // pass this mem block to the compare job
  req->compareBlockSizes[fileIdx] = bytesRead;
  if (req->blocksToCompare.fetch_sub(1) % 2 == 1)
  {
    // Two blocks have been read, spawn a task to compare them
    assert(req->compareBlocks[0]);
    assert(req->compareBlocks[1]);
    int compareResult = req->compareBlockSizes[0] < req->compareBlockSizes[1];
    if (compareResult == 0)
    {
      // Current blocks are the same size so need to compare by content
      Context* newCtx = new Context({ this, nullptr, 0, req });
      mThreadPool.submitWork(sCompareBlocksWork, newCtx); // pass request data further
    }
    else
    {
      assert(0); // Not the same size files
    }
  }

  LOG("(%u) <-AsyncFileComparer::blockReadCallback\n", getCurrentThreadId());
  return status;
}

void AsyncFileComparer::readFinishCallback(Context* ctx)
{
  LOG("(%u) ->AsyncFileComparer::readFinishCallback\n", getCurrentThreadId());
  assert(ctx);

  // Memory block isn't needed anymore for file reading
  mMemBlockPool.releaseMemBlock(ctx->memBlock);
  delete ctx;
  LOG("(%u) <-AsyncFileComparer::readFinishCallback\n", getCurrentThreadId());
}

void AsyncFileComparer::compareBlocksWork(Context* ctx)
{
  assert(ctx);
  Context* context = static_cast<Context*>(ctx);
  CompareRequest* req = ctx->req;
  assert(req);
  WLOG(L"(%u) ->AsyncFileComparer::compareBlocksWork: jobs(%d %d), blocksToCompare = %d\n",
    getCurrentThreadId(), req->jobIds[0], req->jobIds[1], req->blocksToCompare.load());
  assert(req->compareBlockSizes[0] == req->compareBlockSizes[1]);
  int compareResult = std::memcmp(req->compareBlocks[0], req->compareBlocks[1], req->compareBlockSizes[0]);
  mMemBlockPool.releaseMemBlock(req->compareBlocks[0]);
  req->compareBlocks[0] = nullptr;
  mMemBlockPool.releaseMemBlock(req->compareBlocks[1]);
  req->compareBlocks[1] = nullptr;

  if (compareResult == 0 && req->blocksToCompare > 0)
  {
    // Continue comparing
    mIOPool.resume(req->jobIds[0]);
    mIOPool.resume(req->jobIds[1]);
  }
  else
  {
    mIOPool.abort(req->jobIds[0]);
    mIOPool.abort(req->jobIds[1]);
    finishResult({ { req->files[0], req->files[1] }, { compareResult } });
    delete req;
  }
  LOG("(%u) <-AsyncFileComparer::compareBlocksWork\n", getCurrentThreadId());
}

void AsyncFileComparer::finishResult(const Result& result)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mResults.emplace_back(result);
  //mCompareResultMap.insert(result);
  --mOutstandingRequests;
  mCondition.notify_one();
}
