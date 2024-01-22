#pragma once

#include <vector>
#include <string>

#include "ThreadPool.h"
#include "IOPool.h"
#include "FileHasher.h"

class DupFinder
{
public:
  DupFinder(int concurrentIO, int workerThreads);

  void setHashFunction(HashFunction* func);
  std::vector<std::vector<std::string>> findIdentical(const std::string& path);

private:
  ThreadPool mThreadPool;
  IOPool mIoPool;
  FileHasher mHasher;
};
