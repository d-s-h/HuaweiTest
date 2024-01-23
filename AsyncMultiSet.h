#pragma once

#include <vector>
#include <stack>
#include <map>
#include <algorithm>
#include <cassert>

// Node structure for the binary tree
struct MultiSetNode
{
  std::vector<int> keyEntries;
  MultiSetNode* left = nullptr;
  MultiSetNode* right = nullptr;
};

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
  using Queue = std::vector<std::pair<int, int>>;

  AsyncMultiSet() = default;
  AsyncMultiSet(const AsyncMultiSet&) = delete;
  AsyncMultiSet& operator=(const AsyncMultiSet&) = delete;
  AsyncMultiSet(AsyncMultiSet&& other);
  AsyncMultiSet& operator=(AsyncMultiSet&& other);
  ~AsyncMultiSet();

  bool insert(int key);
  AsyncSetIterator getIterator() { return AsyncSetIterator(mRoot); }
  Queue& getNotResolved() { return mQueue; }
  void resolve(const int& l, const int& r, int8_t res)  { mCompareResultMap.insert({ {l, r}, res }); }

private:
  bool insert(MultiSetNode* root, int key);
  void remove(MultiSetNode* node);
  bool getCompare(const int& l, const int& r, int& result);
  void asyncCompare(const int& l, const int& r) { mQueue.push_back({ l , r }); }

  MultiSetNode* mRoot = nullptr;
  std::map< std::pair<int, int>, int8_t> mCompareResultMap;
  Queue mQueue;
};
