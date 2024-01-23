#pragma once

#include <mutex>
#include <vector>

// Memory block allocator.
// Blocks are organized in a pool so can be recycled effectively.
class MemBlockPool
{
public:
	MemBlockPool(uint32_t blockSize, uint32_t blockCount);

	uint32_t getBlockSize() { return mBlockSize; }
	uint32_t getBlockCount() { return static_cast<int>(mFreeBufferBlocks.size()); }

	// Returns a free block immediately if available.
	// Waits for a free block until it's available.
	uint8_t* acquireMemBlock(uint32_t ownerId, int limit);
	void releaseMemBlock(const uint8_t* block);

	// debug purpose
	uint32_t getOwnerId(const uint8_t* block);

private:
	uint32_t mBlockSize = 0;
	std::vector<uint32_t> mFreeBufferBlocks;
	std::vector<uint8_t> mBuffer;

	std::mutex mMutex;
	std::condition_variable mCondition;
};
