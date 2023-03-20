#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

  // Get the value associated with the given key.
  // 1. If the key is not in the trie, return nullptr.
  // 2. If the key is in the trie but the type is mismatched, return nullptr.
  // 3. Otherwise, return the value.
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  std::shared_ptr<const TrieNode> t(root_);
  for(uint64_t i = 0;i < key.length(); i++){
      auto it = t->children_.find(key.at(i));
      if(it == t->children_.end()){
	  return nullptr;
      }
      t = it->second;
      //t = std::shared_ptr<const TrieNode>(it->second);
  }
  if(!(t->is_value_node_)){
      return nullptr;
  }
  return nullptr;
  // auto tmp = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(t);
  //if (!tmp) {
  //    return nullptr;
  //}
  //if (typeid(tmp->value_) != typeid(T)) {
  //    return nullptr;
  //}
  //return tmp->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

  // Put a new key-value pair into the trie. If the key already exists, overwrite the value.
  // Returns the new trie.
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  std::shared_ptr<TrieNode> root = std::shared_ptr<TrieNode>(root_->Clone());
  //std::shared_ptr<TrieNode> root = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
  std::shared_ptr<TrieNode> t(root);
  
  for(uint64_t i = 0;i < key.length(); i++){
      auto it = t->children_.find(key.at(i));
      if(it == t->children_.end()){
          if(i != key.length() - 1){
	      std::shared_ptr<TrieNode> tmp(new TrieNode());
	      //std::shared_ptr<TrieNode> tmp(new TrieNode(key.at(i)));
	      t->children_.insert(std::make_pair(key.at(i),tmp));
              t = tmp;
              //t.reset(tmp);
              //t = std::shared_ptr<TrieNode>(tmp);
	  }else{
              std::shared_ptr<TrieNodeWithValue<T>> tmp = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
              //std::shared_ptr<TrieNodeWithValue> tmp(new TrieNodeWithValue<T>(key.at(i),std::move(value)));
	      t->children_.insert(std::make_pair(key.at(i),tmp));
              //t.reset(tmp);
              t = tmp;
              //t = std::shared_ptr<TrieNode>(tmp);
	  }
      }else{
          if(i == key.length() - 1){
	      //if(it->second.is_value_node){
                //  auto *tmp = dynamic_cast<TrieNodeWithValue<T> *>(t.get());
                  // if(tmp->value_ == value)	break;
	      //}
	      std::shared_ptr<TrieNodeWithValue<T>> node = std::make_shared<TrieNodeWithValue<T>>(it->second->children_,std::make_shared<T>(std::move(value)));
	      //std::shared_ptr<TrieNodeWithValue<T>> node = std::make_shared<TrieNodeWithValue<T>>(it->second->children_,std::shared_ptr<T>(std::move(value)));
	      t->children_.erase(key.at(i));
	      t->children_.insert(std::make_pair(key.at(i),node));
              //t = std::shared_ptr<TrieNode>(node);
              //t.reset(node);
              t = node;
	  }
      }
  }
  Trie res(root);
  return res;
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

  bool RemoveHelper(std::shared_ptr<TrieNode> prev,std::shared_ptr<TrieNode> root, std::string_view key, uint64_t i) {
  //bool RemoveHelper(std::shared_ptr<TrieNode>* new_root,std::shared_ptr<TrieNode> prev,std::shared_ptr<const TrieNode> root, std::string_view key, uint64_t i) {
    auto node = root->children_.find(key.at(i));
    if (node == root->children_.end()){
      return false;
    }
    bool flag = false;
    if (i != key.length() - 1) {
      std::shared_ptr<TrieNode> tmp = std::shared_ptr<TrieNode>(node->second->Clone());
      root->children_.erase(key.at(i));
      root->children_.insert(std::make_pair(key.at(i), tmp));
      flag = RemoveHelper(prev,tmp, key, i + 1);
      //flag = RemoveHelper(root,node, key, i + 1);
    } else {
      if (node->second->is_value_node_) {
        if (!node->second->children_.empty()) {
	  std::shared_ptr<TrieNode> tmp = std::shared_ptr<TrieNode>(node->second->Clone());
	  //std::shared_ptr<TrieNode> tmp = std::shared_ptr<TrieNode>(std::move(node->Clone()));
	  root->children_.erase(key.at(i));
	  root->children_.insert(std::make_pair(key.at(i), tmp));
	  //root->children_.insert(std::make_pair(key.at(i), std::move(tmp)));
        } else {
	  root->children_.erase(key.at(i));
        }
        return true;
      }
      return false;
    }
    auto tmp = root->children_.find(key.at(i));
    if (tmp != root->children_.end() && !(tmp->second->is_value_node_) && (tmp->second->children_.empty())) {
      root->children_.erase(key.at(i));
    }
    return flag;
  }

  // Remove the key from the trie. If the key does not exist, return the original trie.
  // Otherwise, returns the new trie.
auto Trie::Remove(std::string_view key) const -> Trie {
    std::shared_ptr<TrieNode> new_root = std::shared_ptr<TrieNode>(root_->Clone());
    std::shared_ptr<TrieNode> prev = nullptr;
    //std::shared_ptr<TrieNode> new_root = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
    bool res = RemoveHelper(prev,new_root,key,0);
    if(res){
	Trie res_trie(new_root);
	return res_trie;
    }
    return *this;

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
