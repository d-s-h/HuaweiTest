#include "AsyncMultiSet.h"

AsyncMultiSet::AsyncMultiSet(AsyncMultiSet&& other)
{
  std::swap(mRoot, other.mRoot);
  std::swap(mCompareResultMap, other.mCompareResultMap);
  std::swap(mQueue, other.mQueue);
}

AsyncMultiSet& AsyncMultiSet::operator=(AsyncMultiSet&& other)
{
  std::swap(mRoot, other.mRoot);
  std::swap(mCompareResultMap, other.mCompareResultMap);
  std::swap(mQueue, other.mQueue);
  return *this;
}

AsyncMultiSet::~AsyncMultiSet()
{
  remove(mRoot);
}

void AsyncMultiSet::remove(MultiSetNode* node)
{
  if (node)
  {
    remove(node->left);
    remove(node->right);
    delete node;
  }
}

inline bool AsyncMultiSet::getCompare(const int& l, const int& r, int& result)
{
  auto it = mCompareResultMap.find({ l, r });
  if (it != mCompareResultMap.end())
  {
    result = it->second;
    return true;
  }
  return false;
}

bool AsyncMultiSet::insert(int key)
{
  if (mRoot == nullptr)
  {
    MultiSetNode* newNode = new MultiSetNode{ { key }, nullptr, nullptr };
    mRoot = newNode;
    return true;
  }
  else
  {
    return insert(mRoot, key);
  }
}

bool AsyncMultiSet::insert(MultiSetNode* node, int key)
{
  int compareResult = 0;
  if (getCompare(key, node->keyEntries[0], compareResult))
  {
    if (compareResult == 0)
    {
      // Duplicate found, add to the existing key entry.
      node->keyEntries.push_back(key);
      return true;
    }
    else if (compareResult < 0)
    {
      if (node->left)
      {
        return insert(node->left, key);
      }
      else
      {
        MultiSetNode* newNode = new MultiSetNode{ { key }, nullptr, nullptr };
        node->left = newNode;
        return true;
      }
    }
    else
    {
      if (node->right)
      {
        return insert(node->right, key);
      }
      else
      {
        MultiSetNode* newNode = new MultiSetNode{ { key }, nullptr, nullptr };
        node->right = newNode;
        return true;
      }
    }
  }
  else
  {
    // Compare result isn't yet available, add to the queue.
    asyncCompare(key, node->keyEntries[0]);
    return false;
  }
}
