#pragma once

#include <string>
#include <vector>

#include "AsyncFileComparer.h"
#include "AsyncFileHasher.h"
#include "IOPool.h"
#include "ThreadPool.h"

class DupFinder
{
public:
  using Result = std::vector<std::vector<std::string>>;

  DupFinder(int concurrentIO, int workerThreads);

  void setHashFunction(HashFunction* func);
  std::vector<std::vector<std::string>> findIdentical(const std::string& path);

private:
  ThreadPool mThreadPool;
  IOPool mIoPool;
  AsyncFileHasher mFileHasher;
  AsyncFileComparer mFileComparer;
};
