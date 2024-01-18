
#include <vector>
#include <string>

#include "DupFinder.h"

//extern std::vector<std::vector<std::string>> findIdentical(const std::string& path);

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

void testcase1()
{
	TestFile tf[] =
	{
		{ "dir1/dir2/file2", "contentA" },
		{ "dir1/file1", "contentB" },
		{ "dir1/file3", "contentA" },
		{ "dir1/file4", "contentC" }
	};

	//assert();
}