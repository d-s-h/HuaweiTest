#include "DupFinder.h"

#include <vector>
#include <string>

#include "Platform.h"
#include "test.h"

void MyWorkCallback(void* ctx)
{
	int data = static_cast<int>(reinterpret_cast<uintptr_t>(ctx));
  printf("MyWorkCallback: Task performed, data = %d\n", data);
}

int main()
{
	//std::vector<std::vector<std::string>> res = findIdentical("D:\\projects\\HuaweiTest\\test");
	//std::vector<std::vector<std::string>> res = findIdentical("test");
	testsuite();
	//ThreadPool threadPool;
	//threadPool.submitJob(MyWorkCallback, reinterpret_cast<void*>(5));
	//threadPool.waitWorkers();
	return 0;
}
