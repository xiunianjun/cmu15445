#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  // record the root page
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  
  guard = bpm_->FetchPageRead(header_page->root_page_id_);
  const InternalPage* root_page = guard.As<InternalPage>();

  if (root_page->IsLeafPage()) {
    return (root_page->GetSize() == 0);
  }
  return root_page->GetSize() <= 1; // TODO: Can root be one value internal page?
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  BasicPageGuard guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  
  guard = bpm_->FetchPageBasic(header_page->root_page_id_);
  InternalPage* root = guard.AsMut<InternalPage>();

  while (true) {
    if (root->IsLeafPage()) {
      // guard and root is now pointing to the same page, so it is safe here
      auto leaf = guard.As<LeafPage>();
      BUSTUB_ASSERT(leaf->IsLeafPage(), "leaf should be a leaf page!");
      for (int i = 0; i < leaf->GetSize(); i ++) { // for leaf page, traverse the key map begin with 0
        if (comparator_(key, leaf->KeyAt(i)) == 0) { // get target
          result->push_back(static_cast<ValueType>(leaf->ValueAt(i)));
          return true;
        }
      }
      return false; // not get target
    }

    bool flag = false;
    for (int i = 1; i < root->GetSize(); i ++) { // for internal page, traverse the key map begin with 1
      if (comparator_(key, root->KeyAt(i)) < 0) { // if target < current key, target is in the lefthand of key
        guard = bpm_->FetchPageBasic(root->ValueAt(i - 1));
        root = guard.AsMut<InternalPage>(); // goto left child
        flag = true;
        break;
      }
    }
    if (!flag) {
      guard = bpm_->FetchPageBasic(root->ValueAt(root->GetSize() - 1));
      root = guard.AsMut<InternalPage>(); // goto most right child
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
/* 
 * my algorithm: 
 * 1. for split
 * The connections of old node have no need to change, just add a new connection 
 * between the new node and the parent node.
 * 2. for insert
 * The new key-value is always inserted into the new node.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;

  // get write page of root
  ctx.header_page_ = std::move(bpm_->FetchPageWrite(header_page_id_));
  // get root page id
  auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // b+ tree is empty
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    bpm_->NewPageGuarded(&(ctx.root_page_id_));
    header_page->root_page_id_ = ctx.root_page_id_;
    
    // create new root
    auto guard = bpm_->FetchPageWrite(ctx.root_page_id_);
    LeafPage* root = guard.AsMut<LeafPage>(); // a leaf node initially
    root->Init(leaf_max_size_);
    // insert new value
    root->IncreaseSize(1);
    root->SetKeyAt(0, key);
    root->SetValueAt(0, value);
    ctx.header_page_ = std::nullopt;
    return true;
  }

  BUSTUB_ASSERT(ctx.root_page_id_ != INVALID_PAGE_ID, "root page id should be valid.");

  // get root page
  auto guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  InternalPage* root = guard.AsMut<InternalPage>();
  ctx.write_set_.push_back(std::move(guard)); 
  
  // get target leaf node
  while (true) {
    if (root->IsLeafPage()) {
      break;
    }
    bool flag = false;
    for (int i = 1; i < root->GetSize(); i ++) { // for internal page, traverse the key map begin with 1
      if (comparator_(key, root->KeyAt(i)) < 0) { // if target < current key, target is in the lefthand of key
        guard = std::move(bpm_->FetchPageWrite(root->ValueAt(i - 1)));
        root = guard.AsMut<InternalPage>(); // goto left child
        ctx.write_set_.push_back(std::move(guard));
        flag = true;
        break;
      }
    }
    if (!flag) {
      guard = std::move(bpm_->FetchPageWrite(root->ValueAt(root->GetSize() - 1)));
      root = guard.AsMut<InternalPage>(); // goto most right child
      ctx.write_set_.push_back(std::move(guard));
    }
  }

  auto leaf_guard = std::move(ctx.write_set_.back());
  auto leaf = leaf_guard.AsMut<LeafPage>();
  if (!(ctx.write_set_.empty())) {
    ctx.write_set_.pop_back();  // the target leaf was pushed to ctx above
  }

  // check is here first
  for (int i = 0; i < leaf->GetSize(); i++) { // for leaf page, traverse the key map begin with 0
    BUSTUB_ASSERT(leaf->IsLeafPage(), "leaf should be a leaf page!");
    if (comparator_(key, leaf->KeyAt(i)) == 0) { // related key is here
      ctx.write_set_.clear();
      ctx.header_page_ = std::nullopt;
      return false;
    }
  }

  // need to be splited
  bool should_split = (leaf->GetSize() == leaf->GetMaxSize());
  KeyType tmp_key;
  page_id_t page_id;
  int m;

  KeyType insert_key;
  page_id_t insert_val;
  int idx;
  WritePageGuard new_page_guard;
  InternalPage* new_page;

  // Though I think there must be a waiy to merge this case with the below while(),
  // I finally decide not to do a merging for a more clear code.
  if (leaf->GetSize() == leaf->GetMaxSize()) {
    m = leaf->GetSize();
    BUSTUB_ASSERT(m > 0, "leaf size must bigger than zero!");
    tmp_key = leaf->KeyAt((m + 1) / 2);
    insert_key = tmp_key;

    /* split to two nodes first */
    // create new node
    bpm_->NewPageGuarded(&page_id);
    insert_val = page_id;
    auto new_page_guard = bpm_->FetchPageWrite(page_id);
    auto new_page = new_page_guard.AsMut<LeafPage>();
    new_page->Init(leaf_max_size_);
    // allocate the second half of the old node to the new node evenly
    new_page->IncreaseSize(m / 2);
    idx = 0;
    for (int i = (m + 1) / 2; i < m; i ++) { // the next page of the split point will give to the key0 of the new node
      new_page->SetKeyAt(idx, leaf->KeyAt(i));
      new_page->SetValueAt(idx, leaf->ValueAt(i));
      idx ++;
    }
    // shrink old node
    // for that the new root should also remain in the new node, minus m/2 is sufficient here
    leaf->IncreaseSize(-m/2);
    // link in the leaf iterator
    new_page->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(page_id);
    // insert into new node finally
    leaf = std::move(new_page);
    leaf_guard = std::move(new_page_guard);
    
    if(ctx.write_set_.empty()) {  // root should be splited
      goto ROOT_SPLIT;
    }

    // get parent
    guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();

    root = guard.AsMut<InternalPage>();
  }

  if (!should_split) {
    goto INSERTION_END;
  }

  // split up
  do {
    if (root->GetSize() < root->GetMaxSize()) {
      // insert into parent node
      BUSTUB_ASSERT(root->IsLeafPage() == false, "root must not be leaf page in the up split");
      BUSTUB_ASSERT(root->GetSize() > 1, "root must have at least one element!");
      for (int i = 1; i <= root->GetSize(); i++) { // remember that root must be internal node here
        if (i != root->GetSize() && comparator_(insert_key, root->KeyAt(i)) > 0) {
          continue;
        }
        // insert into (i - 1)th position
        root->IncreaseSize(1);
        for (int j = root->GetSize() - 1; j >= i + 1; j --) {
          root->SetKeyAt(j, root->KeyAt(j-1));
          root->SetValueAt(j, root->ValueAt(j-1));
        }
        root->SetKeyAt(i, insert_key);
        root->SetValueAt(i, insert_val);  // remember to point to new page
        break;
      }
      goto INSERTION_END;
    }

    if (ctx.write_set_.empty()) {
      break;
    }

    // need to split
    // get parent of the "root"
    auto parent_guard = std::move(ctx.write_set_.back());
    InternalPage* parent = parent_guard.AsMut<InternalPage>();
    ctx.write_set_.pop_back();

    // split node
    m = root->GetSize();
    BUSTUB_ASSERT(m > 0, "leaf size must bigger than zero!");
    KeyType tmp_key = root->KeyAt((m + 1) / 2);

    // split to two nodes first
    // create new node
    page_id_t page_id;
    bpm_->NewPageGuarded(&page_id);
    new_page_guard = bpm_->FetchPageWrite(page_id);
    new_page = new_page_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);
    // allocate the second half of the old node to the new node evenly
    new_page->IncreaseSize(m / 2);// remember that key0 is null
    idx = 1;
    for (int i = (m + 1) / 2; i < m; i ++) { // the next page of the split point will give to the key0 of the new node
      new_page->SetKeyAt(idx, root->KeyAt(i));
      new_page->SetValueAt(idx, root->ValueAt(i));
      idx ++;
    }
    // shrink old node
    root->IncreaseSize(-m/2);

    for (int i = 1; i <= new_page->GetSize(); i ++) {
      if (i != new_page->GetSize() && comparator_(insert_key, new_page->KeyAt(i)) > 0) {
        continue;
      }
      // insert into (i - 1)th position
      new_page->IncreaseSize(1);
      BUSTUB_ASSERT(new_page->GetSize() > 1, "new_page must have at least one element!");
      for (int j = new_page->GetSize() - 1; j >= i + 1; j --) {
        new_page->SetKeyAt(j, new_page->KeyAt(j - 1));
        new_page->SetValueAt(j, new_page->ValueAt(j - 1));
      }
      new_page->SetKeyAt(i, insert_key);
      new_page->SetValueAt(i, insert_val);
      break;
    }

    root = parent;
    guard = std::move(parent_guard);
    insert_key = tmp_key;
    insert_val = page_id;
  } while (!(ctx.write_set_.empty()));

  if(root->GetSize() == root->GetMaxSize()) { // root should be splited
    m = root->GetSize();
    BUSTUB_ASSERT(m > 0, "leaf size must bigger than zero!");

    tmp_key = root->KeyAt((m + 1) / 2);

    bpm_->NewPageGuarded(&page_id);
    new_page_guard = bpm_->FetchPageWrite(page_id);
    new_page = new_page_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);
    // allocate the second half of the old node to the new node evenly
    new_page->IncreaseSize(m / 2);  // remember that key0 is null
    idx = 1;
    for (int i = (m + 1) / 2; i < m; i ++) { // the next page of the split point will give to the key0 of the new node
      new_page->SetKeyAt(idx, root->KeyAt(i));
      new_page->SetValueAt(idx, root->ValueAt(i));
      idx ++;
    }
    // shrink old node
    root->IncreaseSize(-m/2);

    for (int i = 1; i <= new_page->GetSize(); i ++) {
      if (i != new_page->GetSize() && comparator_(insert_key, new_page->KeyAt(i)) > 0) {
        continue;
      }
      // insert into (i - 1)th position
      new_page->IncreaseSize(1);
      BUSTUB_ASSERT(new_page->GetSize() > 1, "new_page must have at least one element!");
      for (int j = new_page->GetSize() - 1; j >= i + 1; j --) {
        new_page->SetKeyAt(j, new_page->KeyAt(j - 1));
        new_page->SetValueAt(j, new_page->ValueAt(j - 1));
      }
      new_page->SetKeyAt(i, insert_key);
      new_page->SetValueAt(i, insert_val);
      break;
    }

ROOT_SPLIT:
    // create and init new root
    page_id_t new_root_page_id;
    bpm_->NewPageGuarded(&new_root_page_id);
    auto new_root_guard = bpm_->FetchPageWrite(new_root_page_id);
    auto new_root = new_root_guard.AsMut<InternalPage>();
    new_root->Init(internal_max_size_);
    // remember to set key0 pointing to old root
    new_root->SetValueAt(0, ctx.root_page_id_);

    auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_page_id;
    ctx.root_page_id_ = new_root_page_id;

    new_root->IncreaseSize(1);

    // insert into parent node
    new_root->SetKeyAt(1, tmp_key);
    new_root->SetValueAt(1, page_id);
  }

INSERTION_END:
  // insert the target
  BUSTUB_ASSERT(leaf->IsLeafPage(), "leaf should be a leaf page!");
  for (int i = 0; i <= leaf->GetSize(); i ++) {
    if (i != leaf->GetSize() && comparator_(key, leaf->KeyAt(i)) > 0) {
      continue;
    }
    // insert into (i - 1)th position
    leaf->IncreaseSize(1);
    for (int j = leaf->GetSize() - 1; j >= i + 1; j --) {
      leaf->SetKeyAt(j, leaf->KeyAt(j - 1));
      leaf->SetValueAt(j, leaf->ValueAt(j - 1));
    }
    leaf->SetKeyAt(i, key);
    leaf->SetValueAt(i, value);
    break;
  }

  ctx.write_set_.clear();
  ctx.header_page_ = std::nullopt;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  // get root page
  BasicPageGuard guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  auto res_pgid = header_page->root_page_id_;
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID);
  }
  
  guard = bpm_->FetchPageBasic(header_page->root_page_id_);
  InternalPage* root = guard.AsMut<InternalPage>();

  while (true) {
    if (root->IsLeafPage()) {
      return INDEXITERATOR_TYPE(bpm_, res_pgid);
    }

    res_pgid = root->ValueAt(0);
    guard = bpm_->FetchPageBasic(root->ValueAt(0));
    root = guard.AsMut<InternalPage>();
  }

  // theorily unreachable
  BUSTUB_ASSERT(false, "b+ tree Begin() wrong!\n");
  return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto it = Begin();
  while (!(it.IsEnd() || comparator_((*it).first, key) == 0)) {
    ++ it;
  }
  return it;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
  return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = std::move(bpm_->FetchPageWrite(header_page_id_));
  // get root page id
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  return header_page->root_page_id_; 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 1; i < internal->GetSize(); i++) {
      // std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      std::cout << internal->KeyAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
