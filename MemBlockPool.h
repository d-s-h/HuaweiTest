#pragma once

#include <mutex>
#include <vector>

class MemBlockPool
{
public:
	MemBlockPool(uint32_t blockSize, uint32_t blockCount);

	uint32_t getBlockSize() { return mBlockSize; }
	uint32_t getBlockCount() { return static_cast<int>(mFreeBufferBlocks.size()); }

	uint8_t* acquireMemBlock(uint8_t ownerId, int limit);
	void releaseMemBlock(const uint8_t* block);

private:
	uint32_t mBlockSize = 0;
	std::vector<uint8_t> mFreeBufferBlocks;
	std::vector<uint8_t> mBuffer;

	std::mutex mMutex;
	std::condition_variable mCondition;
};
