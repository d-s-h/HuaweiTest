#pragma once

#include <string>
#include <vector>

#include "AsyncFileComparer.h"
#include "AsyncFileHasher.h"
#include "IOPool.h"
#include "ThreadPool.h"

// Searches file duplicates using concurrent computations as much as possible.
// See the cpp file for the implementation details.
class DupFinder
{
public:
  using Result = std::vector<std::vector<std::string>>;

  DupFinder(int concurrentIO, int workerThreads);

  // See Hash.h
  void setHashFunction(HashFunction* func);

  // The main method
  Result findIdentical(const std::string& path);

private:
  ThreadPool mThreadPool;
  IOPool mIoPool;
  AsyncFileHasher mFileHasher;
  AsyncFileComparer mFileComparer;
};
