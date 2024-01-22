#include "AsyncFileComparer.h"

#include <cassert>

AsyncFileComparer::AsyncFileComparer(uint32_t blockSize, uint32_t bufferBlockCount, ThreadPool* threadPool, IOPool* ioPool) :
  mBlockSize(blockSize), mBufferBlockCount(bufferBlockCount), mThreadPool(threadPool), mIOPool(ioPool)
{
}

bool AsyncFileComparer::enqueue(const FileInfo* fi1, const FileInfo* fi2)
{
  std::lock_guard<std::mutex> lock(mMutex);

  if (mCompareResultMap.find({ fi1, fi2 }) != mCompareResultMap.end())
  {
    CompareRequest* req = new CompareRequest;
    req->files[0] = fi1;
    req->files[1] = fi2;
    req->blocksLeft = 2;

    const FileInfo* files[2] = { fi1, fi2 };

    IOJob jobs[2];
    for (uint32_t i = 0; i < 2; ++i)
    {
      jobs[i].filename = files[i]->name;
      jobs[i].buffer = 0;
      jobs[i].bufferSize = 0;
      jobs[i].blockReadCallback = sBlockReadCallback;
      jobs[i].finishCallback = sReadFinishCallback;
      jobs[i].ctx = new Context({ this, i, req });

      mIOPool->submitJob(jobs[i]);
    }
    ++mOutstandingRequests;
    return true;
  }
  return false;
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
  req->compareBlocks[fileIdx] = block;
  req->compareBlockSizes[fileIdx] = bytesRead;
  if (req->blocksLeft.fetch_sub(1) == 1)
  {
    // Both blocks are read, spawn a task to compare them
    mThreadPool->submitWork(sCompareBlocksWork, req);
  }

  // Send file read to a pending state until blocks are compared
  IOStatus status;
  status.action = IOStatus::Action::PAUSE;
  status.buffer = 0;
  status.bufferSize = 0;
  return status;
}

void AsyncFileComparer::readFinishCallback(Context* ctx)
{
  assert(ctx);
}

void AsyncFileComparer::compareBlocksWork(Context* ctx)
{
  CompareRequest* req = ctx->req;
  assert(req);
  assert(req->blocksLeft == 0);

  int compareResult = 0;
  if (req->compareBlockSizes[0] == req->compareBlockSizes[1])
  {
    compareResult = std::memcmp(req->compareBlocks[0], req->compareBlocks[1], req->compareBlockSizes[0]);
  }
  else
  {
    compareResult = req->compareBlockSizes[0] < req->compareBlockSizes[1];
  }
  //req->result = compareResult;

  std::lock_guard<std::mutex> lock(mMutex);
  mResults.push_back({ { req->files[0], req->files[1] }, { compareResult } });
  mCompareResultMap.insert({ { req->files[0], req->files[1] }, { 0 } });
  --mOutstandingRequests;
  mCondition.notify_one();
}