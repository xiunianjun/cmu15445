//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child)) {}

void AggregationExecutor::Init() {
  // initialize
  child_executor_->Init();
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
  has_filled_ = false;
}

auto AggregationExecutor::Next(Tuple *param_tuple, RID *param_rid) -> bool {
  if (!has_filled_) {
    Tuple tuple;
    RID rid;
    bool has_next = false;
    while (child_executor_->Next(&tuple, &rid)) {
      has_next = true;
      if (plan_->GetGroupBys().empty()) {
        AggregateKey agg_key;
        agg_key.group_bys_.emplace_back(TypeId::INTEGER, 1);
        aht_->InsertCombine(agg_key, MakeAggregateValue(&tuple));
      } else {
        aht_->InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
      }
    }

    if (!has_next && plan_->GetGroupBys().empty()) {
      AggregateKey agg_key;
      agg_key.group_bys_.emplace_back(TypeId::INTEGER, 1);
      aht_->InsertCombine(agg_key, MakeAggregateValue(nullptr));
    }

    has_filled_ = true;
    aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
  }

  if (*aht_iterator_ == aht_->End()) {
    return false;
  }

  std::vector<Value> param_tuple_values;
  if (!(plan_->GetGroupBys().empty())) {
    for (auto &k : aht_iterator_->Key().group_bys_) {
      param_tuple_values.push_back(k);
    }
  }

  for (auto &v : aht_iterator_->Val().aggregates_) {
    param_tuple_values.push_back(v);
  }

  *param_tuple = Tuple(param_tuple_values, &(plan_->OutputSchema()));

  ++(*aht_iterator_);
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
