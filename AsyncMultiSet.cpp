#include "AsyncMultiSet.h"

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

bool AsyncMultiSet::insert(int key, IAsyncCompare<int>* comparator)
{
  if (mRoot == nullptr)
  {
    MultiSetNode* newNode = new MultiSetNode{ { key }, nullptr, nullptr };
    mRoot = newNode;
    return true;
  }
  else
  {
    return insert(mRoot, key, comparator);
  }
}

bool AsyncMultiSet::insert(MultiSetNode* node, int key, IAsyncCompare<int>* comparator)
{
  int compareResult = 0;
  if (comparator->getCompare(key, node->keyEntries[0], compareResult))
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
        return insert(node->left, key, comparator);
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
        return insert(node->right, key, comparator);
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
    comparator->asyncCompare(key, node->keyEntries[0]);
    return false;
  }
}
