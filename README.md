# High performance file duplicates finder

## Task descriptions

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


# High-level overview of the program.

* Get a list of all files and their sizes
* Use content compare for same size files only.
* To compare files content feed IOCP with Min(K, N) threads for optimal performance.
* Hash files content once read so compare next files by content only if hash/size are different
* Do I need collision handling: compare by content if hash/size are the same? Heuristics?

Feed IOCP with K files.

Threads to spawn:
Min(K, N) for optimal performance.
If N < K, consider spawning less threads but keep more requests? Not sure how to better handle this using high-level IOCP.
