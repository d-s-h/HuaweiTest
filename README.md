# High performance file duplicates finder

## Task description

Consider N-core machines, and given folder path find all identical (by content) files under it.
I.e. implement a function:
```c++
std::vector<std::vector<std::string>> findIdentical(const string& path) {
    ....
}
```
which returns list of lists, where inner list is the list of all file names with coinciding content:
```
dir1
  |- dir2
  |  |- file2
  |- file1
  |- file3
  |- file4
```
if content of file2 == content of file3, return list like this: `[[dir2/file2, file3], [file1], [file4]]`.

## Special considerations

Please ensure that code uses concurrent computations as much as possible (assume, that IO backend is capable to serve K concurrent IO requests at the same time)
and works well for both large and small files.


# High-level overview of the program

NOTE: This program uses a pedantical way to compare files. It compares file content byte by byte in case of hash collision! This significantly increases time of files processing.

* Get a list of all files and their sizes.
* Use hash compare for unique size files only.
* Use content compare for unique sizes and hash collided files only.

* To calculate hashes files are feed to IOCP with K concurrent IO using N worker threads for hash blocks calculation.
* To compare files content files are feed to IOCP with K concurrent IO using N worker threads for files pair processing.

For implementation details see comments in the source files.

## Assumptions
* Each IO request doesn't occupy a core so at least K + N file buffers are needed to perform K + N operations in the same time.

# Further improvements
* Use more wide hashes like MD5/SHA-256/etc
* Try using a merge sort instead of binary tree search with N * logN potential complexity.
  Now a not balanced binary tree is used which on average is N * logN but can give up to N * N complexity.
* Simplify async code with coroutines or a task graph with dependencies.
* Use block-chain (e.g. hashing of blocks split by page size) to avoid full file content comparison. Not worth usuing because files with the same hash/size are usually the same.
* Add command-line usage
* Better path handling
* Multiplatform support
* Open source