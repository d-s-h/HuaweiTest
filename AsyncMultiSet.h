#pragma once

#include <vector>
#include <stack>
#include <map>
#include <algorithm>
#include <cassert>

// Node structure for the binary tree
template<class T>
struct MultiSetNode
{
  std::vector<T> keyEntries;
  MultiSetNode* left = nullptr;
  MultiSetNode* right = nullptr;
};

// Iterator for inorder traversal of a binary tree
template <class T>
class AsyncSetIterator {
public:
  AsyncSetIterator(MultiSetNode<T>* root)
  {
    pushLeftPath(root);
  }

  bool hasNext() const {
    return !stack.empty();
  }

  std::vector<T>& next()
  {
    MultiSetNode<T>* current = stack.top();
    stack.pop();

    pushLeftPath(current->right);

    return current->keyEntries;
  }

private:
  void pushLeftPath(MultiSetNode<T>* node)
  {
    while (node != nullptr) {
      stack.push(node);
      node = node->left;
    }
  }

  std::stack<MultiSetNode<T>*> stack;
};

// The class works as the std::multiset, i.e. a binary tree which can store mutiple identical key elements in the same node.
// It uses a not balanced binary tree so it's not as effective in search as the std::multiset.
// There is an interface which allows to feed the set with comparision results in the async way.
template<class T>
class AsyncMultiSet
{
public:
  using Queue = std::vector<std::pair<T, T>>;

  AsyncMultiSet() = default;
  AsyncMultiSet(const AsyncMultiSet&) = delete;
  AsyncMultiSet& operator=(const AsyncMultiSet&) = delete;
  AsyncMultiSet(AsyncMultiSet&& other)
  {
    std::swap(mRoot, other.mRoot);
    std::swap(mCompareResultMap, other.mCompareResultMap);
    std::swap(mQueue, other.mQueue);
  }
  AsyncMultiSet& operator=(AsyncMultiSet&& other)
  {
    std::swap(mRoot, other.mRoot);
    std::swap(mCompareResultMap, other.mCompareResultMap);
    std::swap(mQueue, other.mQueue);
    return *this;
  }

  ~AsyncMultiSet()
  {
    remove(mRoot);
  }

  // Returns false if couldn't insert an element.
  // The failure element is stored in the not resolved queque.
  bool insert(T key)
  {
    if (mRoot == nullptr)
    {
      MultiSetNode<T>* newNode = new MultiSetNode<T>{ { key }, nullptr, nullptr };
      mRoot = newNode;
      return true;
    }
    else
    {
      return insert(mRoot, key);
    }
  }

  AsyncSetIterator<T> getIterator() { return AsyncSetIterator(mRoot); }

  // Obtain an array of failed to insert elements.
  Queue& getNotResolved() { return mQueue; }

  // Feed with a comparison result for a particular pair of keys.
  void resolve(const T& l, const T& r, int res)  { mCompareResultMap.insert({ {l, r}, res }); }

private:
  bool insert(MultiSetNode<T>* node, T key)
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
          MultiSetNode<T>* newNode = new MultiSetNode<T>{ { key }, nullptr, nullptr };
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
          MultiSetNode<T>* newNode = new MultiSetNode<T>{ { key }, nullptr, nullptr };
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

  void remove(MultiSetNode<T>* node)
  {
    if (node)
    {
      remove(node->left);
      remove(node->right);
      delete node;
    }
  }

  bool getCompare(const T& l, const T& r, int& result)
  {
    auto it = mCompareResultMap.find({ l, r });
    if (it != mCompareResultMap.end())
    {
      result = it->second;
      return true;
    }
    return false;
  }

  void asyncCompare(const T& l, const T& r) { mQueue.push_back({ l , r }); }

  MultiSetNode<T>* mRoot = nullptr;
  std::map< std::pair<T, T>, int> mCompareResultMap;
  Queue mQueue;
};
