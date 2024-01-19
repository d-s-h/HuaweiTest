#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <set>

struct FileInfo
{
  uint64_t size = 0;
  uint64_t contentHash = 0;
  bool hashed = false;
  std::wstring name;
};

using FileInfoMap = std::unordered_multimap<uint64_t, FileInfo>;

struct DupSetEntry
{
  std::vector<const FileInfo*> dupList;
};

bool ContentCompare(const DupSetEntry& fiLeft, const DupSetEntry& fiRight);

struct SizeHashKey
{
  uint64_t size = 0;
  uint64_t hash = 0;
};

struct SizeHashKeyHash
{
  size_t operator()(const SizeHashKey& key) const
  {
    return key.hash ^ key.size;
  }
};

using DupSet = std::set<DupSetEntry, decltype(ContentCompare)>;

struct SizeHashEntry
{
  SizeHashEntry(){}
  std::vector<const FileInfo*> files;
  DupSet* dupSet;
};


using FileHashMap = std::unordered_map<SizeHashKey, SizeHashEntry, SizeHashKeyHash>;

class IFileSys
{
public:
  IFileSys() = default;
  uint64_t GetFileSize(const std::string& name);
  bool ReadFile(uint8_t* buffer, uint64_t size);

  ~IFileSys();
};

using HashFunction = uint64_t(const void* key, int len, uint64_t seed);
void setHashFunction(HashFunction* func);
std::vector<std::vector<std::string>> findIdentical(const std::string& path);


