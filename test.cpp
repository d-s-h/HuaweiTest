
#include <vector>
#include <string>
#include <map>
#include <cassert>

#include <intrin.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <cstdlib>

#include "DupFinder.h"
#include "AsyncMultiSet.h"
#include "Hash.h"


//extern std::vector<std::vector<std::string>> findIdentical(const std::string& path);

#define TEST_ASSERT(cond, format, ...) \
  do { \
      if(!(cond)) { \
					printf("TEST_ASSERT " #cond " in " "%s(%d):%s : " format "\n", \
					__FILE__, __LINE__, __func__, ##__VA_ARGS__); \
					__debugbreak(); \
					return false; \
			} \
  } while(false);


void generateFile(std::filesystem::path path, size_t size, unsigned int randomSeed)
{
	std::filesystem::create_directories(path.parent_path());
	std::fstream file;
	file.open(path, std::ios::app | std::ios::binary);
	std::srand(randomSeed);
	uint32_t block = 0;
	uint32_t blockSize = sizeof(block);
	size_t blockCount = size / blockSize;
	for(size_t i = 0; i < blockCount; ++i)
	{
		block = std::rand();
		block <<= 16;
		block |= std::rand();
		file.write(reinterpret_cast<char*>(&block), blockSize);
	}

	uint32_t rem = size % blockSize;
	if (rem > 0)
	{
		block = std::rand();
		block <<= 16;
		block |= std::rand();
		file.write(reinterpret_cast<char*>(&block), rem);
	}

	file.close();
}

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

bool testcase1()
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

	return true;
}

// Differ by size
// The same size are identical
bool testcase2()
{
	const std::string testFolder = "test_gen\\";
	// Clean up previous runs if any
	std::filesystem::remove_all(testFolder);

	// Setup test
	size_t MB = 1024 * 1024;
	generateFile(testFolder + "gen1.bin", 3 * MB, 0);
	generateFile(testFolder + "gen11.bin", 3 * MB, 0);
	generateFile(testFolder + "gen2.bin", 4 * MB, 0);
	generateFile(testFolder + "gen3.bin", 5 * MB, 0);
	generateFile(testFolder + "gen4.bin", 5 * MB, 0);

	// Test
	std::vector<std::vector<std::string>> fileList = findIdentical(testFolder);

	TEST_ASSERT(fileList.size() == 3, "");
	TEST_ASSERT(fileList[0].size() == 2, "");
	TEST_ASSERT(fileList[0][0] == "gen1.bin", "");
	TEST_ASSERT(fileList[0][1] == "gen11.bin", "");
	TEST_ASSERT(fileList[1].size() == 1, "");
	TEST_ASSERT(fileList[1][0] == "gen2.bin", "");
	TEST_ASSERT(fileList[2].size() == 2, "");
	TEST_ASSERT(fileList[2][0] == "gen3.bin", "");
	TEST_ASSERT(fileList[2][1] == "gen4.bin", "");

	return true;
}

// Differ by size
// Content is different
// Hashes are different
bool testcase3()
{
	const std::string testFolder = "test_gen\\";
	// Clean up previous runs if any
	std::filesystem::remove_all(testFolder);

	// Setup test
	size_t MB = 1024 * 1024;
	generateFile(testFolder + "gen1.bin", 3 * MB, 0);
	generateFile(testFolder + "gen2.bin", 3 * MB, 837462);	// Same size but different content
	generateFile(testFolder + "gen3.bin", 3 * MB, 374852);	// Same size but different content
	generateFile(testFolder + "gen4.bin", 3 * MB, 938456);	// Same size but different content

	// Test
	//setHashFunction(simpleHash);
	std::vector<std::vector<std::string>> fileList = findIdentical(testFolder);

	TEST_ASSERT(fileList.size() == 4, "");
	
	TEST_ASSERT(fileList[0].size() == 1, "");
	TEST_ASSERT(fileList[0][0] == "gen1.bin", "");
	TEST_ASSERT(fileList[1].size() == 1, "");
	TEST_ASSERT(fileList[1][0] == "gen2.bin", "");
	TEST_ASSERT(fileList[2].size() == 1, "");
	TEST_ASSERT(fileList[2][0] == "gen3.bin", "");
	TEST_ASSERT(fileList[3].size() == 1, "");
	TEST_ASSERT(fileList[3][0] == "gen4.bin", "");

	return true;
}

// Differ by size
// Content is different
// Hashes are identical
bool testcase4()
{
	const std::string testFolder = "test_gen\\";
	// Clean up previous runs if any
	std::filesystem::remove_all(testFolder);

	// Setup test
	size_t MB = 1024 * 1024;
	generateFile(testFolder + "gen1.bin", 3 * MB, 0);
	generateFile(testFolder + "gen2.bin", 3 * MB, 837462);	// Same size but different content
	generateFile(testFolder + "gen3.bin", 3 * MB, 374852);	// Same size but different content
	generateFile(testFolder + "gen4.bin", 3 * MB, 938456);	// Same size but different content

	// Test
	setHashFunction(constantHash);
	std::vector<std::vector<std::string>> fileList = findIdentical(testFolder);

	TEST_ASSERT(fileList.size() == 4, "");

	TEST_ASSERT(fileList[0].size() == 1, "");
	TEST_ASSERT(fileList[0][0] == "gen1.bin", "");
	TEST_ASSERT(fileList[1].size() == 1, "");
	TEST_ASSERT(fileList[1][0] == "gen2.bin", "");
	TEST_ASSERT(fileList[2].size() == 1, "");
	TEST_ASSERT(fileList[2][0] == "gen3.bin", "");
	TEST_ASSERT(fileList[3].size() == 1, "");
	TEST_ASSERT(fileList[3][0] == "gen4.bin", "");

	return true;
}

void testsuite()
{
	using FuncPtr = bool();
	//void (*funcPtr)();
	//int tests = 0;
	int testPassed = 0;
	FuncPtr* tests[] = {
		//testcase1,
		testcase2,
		testcase3,
		testcase4
	};
	
	for (auto& t : tests)
	{
		testPassed += t();
	}
	
	int testCount = sizeof(tests) / sizeof(tests[0]);

	printf("Tests Passed %d/%d\n", testPassed, testCount);
}
