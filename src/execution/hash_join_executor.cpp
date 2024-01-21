//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID rid;
  has_end_ = false;
  if (!(left_executor_->Next(&current_left_tuple_, &rid))) {
    has_end_ = true;
  }
  is_init_ = false;
  cursor_right_ = 0;
  ht_.clear();
}

auto HashJoinExecutor::Next(Tuple *param_tuple, RID *param_rid) -> bool {
  if (has_end_) {
    return false;
  }

  // build the hash table
  if (!is_init_) {
    Tuple tuple;
    RID rid;
    while (right_executor_->Next(&tuple, &rid)) {
      auto key = GetKeyFromExpressions(plan_->RightJoinKeyExpressions(), &tuple, plan_->GetRightPlan()->OutputSchema());
      auto it = ht_.find(key);
      if (it != ht_.end()) {
        it->second.build_tuples_.emplace_back(tuple);
      } else {
        HashJoinValue val;
        std::vector<Tuple> tmp_vec;
        tmp_vec.emplace_back(tuple);
        val.build_tuples_ = std::move(tmp_vec);
        ht_.insert(std::make_pair(key, std::move(val)));
      }
    }

    is_init_ = true;
  }

  Tuple right_tuple;
  RID rid;
  std::vector<Value> insert_vals;

  while (true) {
    insert_vals.clear();
    for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumns().size(); i++) {
      insert_vals.push_back(current_left_tuple_.GetValue(&(plan_->GetLeftPlan()->OutputSchema()), i));
    }

    // try to get the shrinked area
    auto key = GetKeyFromExpressions(plan_->LeftJoinKeyExpressions(), &current_left_tuple_,
                                     plan_->GetLeftPlan()->OutputSchema());
    auto it = ht_.find(key);
    if (it != ht_.end() && cursor_right_ < it->second.build_tuples_.size()) {
      // spawn
      right_tuple = it->second.build_tuples_[cursor_right_++];
      break;
    }

    uint64_t old_cursor = cursor_right_;
    cursor_right_ = 0;

    if (!(left_executor_->Next(&current_left_tuple_, &rid))) {
      has_end_ = true;
    }

    if ((old_cursor == 0) && (plan_->GetJoinType() == JoinType::LEFT)) {
      // spawn a left join
      std::vector<Value> tmp_vals;
      for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumns().size(); i++) {
        tmp_vals.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
      right_tuple = Tuple(tmp_vals, &(plan_->GetRightPlan()->OutputSchema()));
      break;
    }

    if (has_end_) {
      return false;
    }
  }

  for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumns().size(); i++) {
    insert_vals.push_back(right_tuple.GetValue(&(plan_->GetRightPlan()->OutputSchema()), i));
  }

  *param_tuple = Tuple(insert_vals, &(GetOutputSchema()));

  return true;
}

}  // namespace bustub
