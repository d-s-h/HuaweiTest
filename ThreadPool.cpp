#include "ThreadPool.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <tchar.h>
#include <cassert>
#include <iostream>

struct WorkItem
{
  WorkCallbackFn* cb;
  void* ctx;
};

class ThreadPoolImpl
{
public:
  ThreadPoolImpl(int threadCount);
  ~ThreadPoolImpl();

  bool submitWork(WorkCallbackFn* cb, void* ctx);
  void waitWorkers();
private:
  PTP_POOL pool = NULL;
  TP_CALLBACK_ENVIRON CallBackEnviron;
  PTP_CLEANUP_GROUP cleanupgroup = NULL;
};

ThreadPoolImpl::ThreadPoolImpl(int threadCount)
{
  BOOL bRet = FALSE;
  UINT rollback = 0;

  InitializeThreadpoolEnvironment(&CallBackEnviron);

  //
  // Create a custom, dedicated thread pool.
  //
  pool = CreateThreadpool(NULL);

  if (NULL == pool) {
    _tprintf(_T("CreateThreadpool failed. LastError: %u\n"),
      GetLastError());
    goto main_cleanup;
  }

  rollback = 1; // pool creation succeeded

  //
  // The thread pool is made persistent simply by setting
  // both the minimum and maximum threads to 1.
  //
  SetThreadpoolThreadMaximum(pool, threadCount);

  bRet = SetThreadpoolThreadMinimum(pool, threadCount);

  if (FALSE == bRet) {
    _tprintf(_T("SetThreadpoolThreadMinimum failed. LastError: %u\n"),
      GetLastError());
    goto main_cleanup;
  }

  //
  // Create a cleanup group for this thread pool.
  //
  cleanupgroup = CreateThreadpoolCleanupGroup();

  if (NULL == cleanupgroup) {
    _tprintf(_T("CreateThreadpoolCleanupGroup failed. LastError: %u\n"),
      GetLastError());
    goto main_cleanup;
  }

  rollback = 2;  // Cleanup group creation succeeded

  //
  // Associate the callback environment with our thread pool.
  //
  SetThreadpoolCallbackPool(&CallBackEnviron, pool);

  //
  // Associate the cleanup group with our thread pool.
  // Objects created with the same callback environment
  // as the cleanup group become members of the cleanup group.
  //
  SetThreadpoolCallbackCleanupGroup(&CallBackEnviron,
    cleanupgroup,
    NULL);

  return;

main_cleanup:
  //
  // Clean up any individual pieces manually
  // Notice the fall-through structure of the switch.
  // Clean up in reverse order.
  //

  switch (rollback) {
  case 4:
  case 3:
    // Clean up the cleanup group members.
    CloseThreadpoolCleanupGroupMembers(cleanupgroup,
      FALSE, NULL);
  case 2:
    // Clean up the cleanup group.
    CloseThreadpoolCleanupGroup(cleanupgroup);

  case 1:
    // Clean up the pool.
    CloseThreadpool(pool);

  default:
    break;
  }
}

ThreadPoolImpl::~ThreadPoolImpl()
{
  //
  // Wait for all callbacks to finish.
  // CloseThreadpoolCleanupGroupMembers also releases objects
  // that are members of the cleanup group, so it is not necessary 
  // to call close functions on individual objects 
  // after calling CloseThreadpoolCleanupGroupMembers.
  //
  CloseThreadpoolCleanupGroupMembers(cleanupgroup, FALSE, NULL);
  CloseThreadpoolCleanupGroup(cleanupgroup);
  CloseThreadpool(pool);
}


//
// This is the thread pool work callback function.
//
VOID CALLBACK WorkCallback(
  PTP_CALLBACK_INSTANCE Instance,
  PVOID                 Parameter,
  PTP_WORK              Work
)
{
  // Instance, Parameter, and Work not used in this example.
  UNREFERENCED_PARAMETER(Instance);
  UNREFERENCED_PARAMETER(Work);

  WorkItem* wi = static_cast<WorkItem*>(Parameter);
  assert(wi);
  wi->cb(wi->ctx);

  // Cleanup allocated work item
  delete wi;

  return;
}

bool ThreadPoolImpl::submitWork(WorkCallbackFn* cb, void* ctx)
{
  WorkItem* wi = new WorkItem;
  PTP_WORK work = CreateThreadpoolWork(WorkCallback, wi, &CallBackEnviron);

  if (NULL == work) {
    printf(("CreateThreadpoolWork failed. LastError: %u\n"), GetLastError());
    return false;
  }

  wi->ctx = ctx;
  wi->cb = cb;

  SubmitThreadpoolWork(work);

  return true;
}

void ThreadPoolImpl::waitWorkers()
{
  CloseThreadpoolCleanupGroupMembers(cleanupgroup, FALSE, NULL);
}

ThreadPool::ThreadPool(int threadCount)
{
  mImpl.reset(new ThreadPoolImpl(threadCount));
}

ThreadPool::~ThreadPool()
{
}

bool ThreadPool::submitWork(WorkCallbackFn* cb, void* ctx)
{
  return mImpl->submitWork(cb, ctx);
}

void ThreadPool::waitWorkers()
{
  mImpl->waitWorkers();
}