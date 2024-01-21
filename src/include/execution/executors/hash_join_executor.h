//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/** HashJoinKey represents a key in an aggregation operation */
struct HashJoinKey {
  /** The expression values of build table */
  std::vector<Value> build_keys_;

  HashJoinKey() = default;
  /**
   * Compares two hash join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.build_keys_.size(); i++) {
      if (build_keys_[i].CompareEquals(other.build_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** HashJoinValue represents a value for each of the running hash joins */
struct HashJoinValue {
  HashJoinValue() = default;
  /** The hash join values */
  std::vector<Tuple> build_tuples_;
};

}  // namespace bustub

namespace std {

/** Implements std::hash on HashJoinKey */
// it seems a ambiguous hash... maybe i should change my design
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.build_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto GetKeyFromExpressions(const std::vector<AbstractExpressionRef> &expressions, Tuple *tuple, const Schema &schema)
      -> HashJoinKey {
    std::vector<Value> keys;
    keys.reserve(expressions.size());
    for (auto &exp : expressions) {
      keys.push_back(exp->Evaluate(tuple, schema));
    }
    return {keys};
  }

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  Tuple current_left_tuple_;
  bool is_init_;
  bool has_end_;
  uint64_t cursor_right_{0};
  std::unordered_map<HashJoinKey, HashJoinValue> ht_{};
};

}  // namespace bustub
