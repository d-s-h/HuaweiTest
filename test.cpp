
#include <vector>
#include <string>
#include <map>
#include <cassert>

#include <intrin.h>
#include <iostream>

#include "DupFinder.h"
#include "AsyncMultiSet.h"

//extern std::vector<std::vector<std::string>> findIdentical(const std::string& path);

#define TEST_ASSERT(cond, format, ...) \
  do { \
      if(!(cond)) { \
					printf("TEST_ASSERT " #cond " in " "%s(%d):%s : " format "\n", \
					__FILE__, __LINE__, __func__, ##__VA_ARGS__); \
					__debugbreak();\
			} \
  } while(false);

/*

Testcases:
* different files by size
* different files by content with same size
* different files by content with same size and hash
* calc hash for files when size isn't a multiple of a hash block
*/

struct TestFile
{
	std::string name;
	std::string content;
};

class DataComparer : public IAsyncCompare<int>
{
public:
	using Queue = std::vector<std::pair<int, int>>;

	DataComparer() {}

	bool getCompare(const int& l, const int& r, int& result) override
	{
		auto it = mCompareResultMap.find({ l, r });
		if (it != mCompareResultMap.end())
		{
			result = it->second;
			return true;
		}
		return false;
	}

	void asyncCompare(const int& l, const int& r) override
	{
		mQueue.push_back({l , r});
	}

	void addCompareResult(const int& l, const int& r, int8_t res)
	{
		mCompareResultMap.insert({ {l, r}, res });
	}

	Queue& getQueue() { return mQueue; }

private:
	std::map< std::pair<int, int>, int8_t> mCompareResultMap;
	Queue mQueue;
};

void testcase1()
{
	std::vector<std::string> content =
	{
		"contentA",
		"contentB",
		"contentC",
		"contentD",
		"contentA",
		"contentA",
		"contentC"
	};

	std::vector<bool> insert1 =
	{
		true,
		false,
		false,
		false,
		false,
		false,
		false
	};

	AsyncMultiSet asyncSet;
	DataComparer comparer;

	// Iteration 1
	for (int i = 0; i < content.size(); ++i)
	{
		bool res = asyncSet.insert(i, &comparer);
		TEST_ASSERT(res == insert1[i], "");
	}

	TEST_ASSERT(comparer.getQueue().size() == 6, "");

	std::vector<int> toInsert;
	for (auto& e : comparer.getQueue())
	{
		std::string& s1 = content[e.first];
		std::string& s2 = content[e.second];
		int res = s1.compare(s2);
		comparer.addCompareResult(e.first, e.second, res);
		toInsert.push_back(e.first);
	}

	std::vector<bool> insert2 =
	{
		true,
		false,
		false,
		true,
		true,
		false
	};

	comparer.getQueue().clear();

	// Iteration 2
	for (int i = 0; i < toInsert.size(); ++i)
	{
		bool res = asyncSet.insert(toInsert[i], &comparer);
		TEST_ASSERT(res == insert2[i], "i = %d", i);
	}

	TEST_ASSERT(comparer.getQueue().size() == 3, "");

	std::vector<bool> insert3 =
	{
		true,
		false,
		false
	};

	toInsert.clear();

	// Iteration 3
	for (auto& e : comparer.getQueue())
	{
		std::string& s1 = content[e.first];
		std::string& s2 = content[e.second];
		int res = s1.compare(s2);
		comparer.addCompareResult(e.first, e.second, res);
		toInsert.push_back(e.first);
	}

	comparer.getQueue().clear();
	for (int i = 0; i < toInsert.size(); ++i)
	{
		bool res = asyncSet.insert(toInsert[i], &comparer);
		TEST_ASSERT(res == insert3[i], "i = %d", i);
	}

	TEST_ASSERT(comparer.getQueue().size() == 2, "");

	// Iteration 4
	std::vector<bool> insert4 =
	{
		true,
		true
	};

	toInsert.clear();
	for (auto& e : comparer.getQueue())
	{
		std::string& s1 = content[e.first];
		std::string& s2 = content[e.second];
		int res = s1.compare(s2);
		comparer.addCompareResult(e.first, e.second, res);
		toInsert.push_back(e.first);
	}
	comparer.getQueue().clear();
	for (int i = 0; i < toInsert.size(); ++i)
	{
		bool res = asyncSet.insert(toInsert[i], &comparer);
		TEST_ASSERT(res == insert4[i], "i = %d", i);
	}

	TEST_ASSERT(comparer.getQueue().empty(), "The async queue must be empty at this stage");

	AsyncSetIterator it = asyncSet.getIterator();
	TEST_ASSERT(it.hasNext(), "");
	std::vector<int>& keys1 = it.next();
	TEST_ASSERT(keys1.size() == 3, "");
	TEST_ASSERT(keys1[0] == 0, "");
	TEST_ASSERT(keys1[1] == 4, "");
	TEST_ASSERT(keys1[2] == 5, "");

	TEST_ASSERT(it.hasNext(), "");
	std::vector<int>& keys2 = it.next();
	TEST_ASSERT(keys2.size() == 1, "");
	TEST_ASSERT(keys2[0] == 1, "");

	TEST_ASSERT(it.hasNext(), "");
	std::vector<int>& keys3 = it.next();
	TEST_ASSERT(keys3.size() == 2, "");
	TEST_ASSERT(keys3[0] == 2, "");
	TEST_ASSERT(keys3[1] == 6, "");

	TEST_ASSERT(it.hasNext(), "");
	std::vector<int>& keys4 = it.next();
	TEST_ASSERT(keys4.size() == 1, "");
	TEST_ASSERT(keys4[0] == 3, "");

	TEST_ASSERT(!it.hasNext(), "");
}

void testsuite()
{
	testcase1();
}
