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
  AsyncSetIterator(MultiSetNode<T>* root) {
    // Initialize the stack with the leftmost path from the root
    pushLeftPath(root);
  }

  // Check if there are more elements to iterate
  bool hasNext() const {
    return !stack.empty();
  }

  // Get the next element in the inorder traversal
  std::vector<int>& next() {
    // The top of the stack contains the next element
    MultiSetNode<T>* current = stack.top();
    stack.pop();

    // Move to the right subtree to process its leftmost path
    pushLeftPath(current->right);

    return current->keyEntries;
  }

private:
  // Helper function to push the leftmost path of a subtree onto the stack
  void pushLeftPath(MultiSetNode<T>* node) {
    while (node != nullptr) {
      stack.push(node);
      node = node->left;
    }
  }

  std::stack<MultiSetNode<T>*> stack;
};

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
  Queue& getNotResolved() { return mQueue; }
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
