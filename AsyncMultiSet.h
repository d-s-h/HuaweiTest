#pragma once

#include <vector>
#include <stack>
#include <map>

struct MultiSetNode
{
  std::vector<int> keyEntries;
  MultiSetNode* left = nullptr;
  MultiSetNode* right = nullptr;
};

template<typename T>
class IAsyncCompare
{
public:
  virtual ~IAsyncCompare() {}

  virtual bool getCompare(const T& l, const T& r, int& result) = 0;
  virtual void asyncCompare(const T&, const T&) = 0;
};

// Node structure for the binary tree

// Iterator for inorder traversal of a binary tree
class AsyncSetIterator {
public:
  AsyncSetIterator(MultiSetNode* root) {
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
    MultiSetNode* current = stack.top();
    stack.pop();

    // Move to the right subtree to process its leftmost path
    pushLeftPath(current->right);

    return current->keyEntries;
  }

private:
  // Helper function to push the leftmost path of a subtree onto the stack
  void pushLeftPath(MultiSetNode* node) {
    while (node != nullptr) {
      stack.push(node);
      node = node->left;
    }
  }

  std::stack<MultiSetNode*> stack;
};

class AsyncMultiSet
{
public:
  ~AsyncMultiSet();
  bool insert(int key, IAsyncCompare<int>* comparator);
  AsyncSetIterator getIterator() { return AsyncSetIterator(mRoot); }

private:
  bool insert(MultiSetNode* root, int key, IAsyncCompare<int>* comparator);
  void remove(MultiSetNode* node);
  MultiSetNode* mRoot = nullptr;
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
    mQueue.push_back({ l , r });
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