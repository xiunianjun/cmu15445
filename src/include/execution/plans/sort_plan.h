//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_plan.h
//
// Identification: src/include/execution/plans/sort_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The SortPlanNode represents a sort operation. It will sort the input with
 * the given predicate.
 */
class SortPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new SortPlanNode instance.
   * @param output The output schema of this sort plan node
   * @param child The child plan node
   * @param order_bys The sort expressions and their order by types.
   */
  SortPlanNode(SchemaRef output, AbstractPlanNodeRef child,
               std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
      : AbstractPlanNode(std::move(output), {std::move(child)}), order_bys_(std::move(order_bys)) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Sort; }

  /** @return The child plan node */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Sort should have exactly one child plan.");
    return GetChildAt(0);
  }

  /** @return Get sort by expressions */
  auto GetOrderBy() const -> const std::vector<std::pair<OrderByType, AbstractExpressionRef>> & { return order_bys_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(SortPlanNode);

  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;

 protected:
  auto PlanNodeToString() const -> std::string override;
};

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

}  // namespace bustub
