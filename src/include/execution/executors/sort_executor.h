//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct CompareTuplesByOrder {
  std::shared_ptr<Schema> schema_;
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;

  CompareTuplesByOrder(const Schema &schema,
                       const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by) {
    for (auto &o : order_by) {
      order_bys_.push_back(o);
    }

    schema_ = std::make_shared<Schema>(schema);
  }

  // if t1 == t2, t1 always orders before t2
  auto operator()(const Tuple &t1, const Tuple &t2) const -> bool {
    // in order of priority
    for (auto &order : order_bys_) {
      if (order.first == OrderByType::INVALID) {
        continue;
      }

      auto v1 = order.second->Evaluate(&t1, *schema_);
      auto v2 = order.second->Evaluate(&t2, *schema_);

      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }

      if (order.first == OrderByType::DESC) {
        return v1.CompareGreaterThanEquals(v2) == CmpBool::CmpTrue;
      }

      return v1.CompareLessThanEquals(v2) == CmpBool::CmpTrue;
    }

    return true;
  }
};

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_;
  std::vector<Tuple> tuples_;
  bool is_init_;
  uint64_t cursor_{0};
};

}  // namespace bustub
