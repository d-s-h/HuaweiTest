#include "MemBlockPool.h"

#include <cassert>
#include "Platform.h"

MemBlockPool::MemBlockPool(uint32_t blockSize, uint32_t blockCount):
  mBlockSize(blockSize)
{
  mFreeBufferBlocks.resize(blockCount, 0);
  mBuffer.resize(blockCount * blockSize);
}

uint8_t* MemBlockPool::acquireMemBlock(uint32_t ownerId, int limit)
{
  LOG("(%u) ->MemBlockPool::acquireMemBlock: owner %u\n", getCurrentThreadId(), ownerId);
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
  LOG("(%u) <-MemBlockPool::acquireMemBlock owner %u 0x%p\n", getCurrentThreadId(), ownerId, block);
  return block;
}

void MemBlockPool::releaseMemBlock(const uint8_t* block)
{
  assert(block != nullptr);
  size_t idx = (block - mBuffer.data()) / mBlockSize;
  assert(idx >= 0 && idx < mFreeBufferBlocks.size());
  std::lock_guard<std::mutex> lock(mMutex);
  LOG("(%u) ->MemBlockPool::releaseMemBlock: idx %llu owner %u 0x%p\n", getCurrentThreadId(), idx, mFreeBufferBlocks[idx], block);
  assert(mFreeBufferBlocks[idx] > 0);
  mFreeBufferBlocks[idx] = 0;
  mCondition.notify_one(); // notify that a block has been release so it can be acquired again.
  LOG("(%u) <-MemBlockPool::releaseMemBlock\n", getCurrentThreadId());
}

uint32_t MemBlockPool::getOwnerId(const uint8_t* block)
{
  size_t idx = (block - mBuffer.data()) / mBlockSize;
  assert(idx >= 0 && idx < mFreeBufferBlocks.size());
  return mFreeBufferBlocks[idx];
}
