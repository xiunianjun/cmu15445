#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::CheckIfEquiConjunction(const AbstractExpressionRef &expr,
                                       std::vector<AbstractExpressionRef> *left_key_expressions,
                                       std::vector<AbstractExpressionRef> *right_key_expressions) -> bool {
  // check child first
  for (const auto &child : expr->GetChildren()) {
    bool res = CheckIfEquiConjunction(child, left_key_expressions, right_key_expressions);
    // if one child invalid, than the whole expr invalid
    if (!res) {
      return false;
    }
  }

  // then check current node
  // if is the column expr
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      column_value_expr != nullptr) {
    BUSTUB_ASSERT(column_value_expr->GetTupleIdx() <= 1, "the column expr tuple idx must be 0 or 1.");
    if (column_value_expr->GetTupleIdx() == 0) {
      left_key_expressions->push_back(std::make_shared<ColumnValueExpression>(
          column_value_expr->GetTupleIdx(), column_value_expr->GetColIdx(), column_value_expr->GetReturnType()));
    } else {
      right_key_expressions->push_back(std::make_shared<ColumnValueExpression>(
          column_value_expr->GetTupleIdx(), column_value_expr->GetColIdx(), column_value_expr->GetReturnType()));
    }
    return true;
  }

  // if is the comparison expr
  if (const auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      comparison_expr != nullptr && comparison_expr->comp_type_ == ComparisonType::Equal) {
    return true;
  }

  // if is the logic expr
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    return true;
  }

  // else, return false
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  // do for its child first, from buttom to up
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 2, "NLJ should have exactly 2 children.");

    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    if (nlj_plan.Predicate() != nullptr &&
        CheckIfEquiConjunction(nlj_plan.Predicate(), &left_key_expressions, &right_key_expressions)) {
      // swicth to a hash join
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                std::move(right_key_expressions), nlj_plan.GetJoinType());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
