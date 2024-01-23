#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

void OptimizerHelperFunction() {}

}  // namespace bustub

namespace bustub {

auto Optimizer::CheckIfColumnConstantConjunction(const AbstractExpressionRef &expr,
                                                 std::vector<uint32_t> *column_indexes,
                                                 std::vector<Value> *first_values, std::vector<Value> *last_values)
    -> bool {
  // if is "AND"
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    // check children
    return std::all_of(expr->GetChildren().begin(), expr->GetChildren().end(),
                       [this, column_indexes, first_values, last_values](const AbstractExpressionRef &child) {
                         return CheckIfColumnConstantConjunction(child, column_indexes, first_values, last_values);
                       });
  }

  // if is the comparison expr
  if (const auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      comparison_expr != nullptr) {
    // check that the children must be constant and column
    BUSTUB_ASSERT(comparison_expr->GetChildren().size() == 2, "Comparison expr must have exactly two children.");
    if (const auto *column_value_expr =
            dynamic_cast<const ColumnValueExpression *>(comparison_expr->GetChildren()[0].get());
        column_value_expr != nullptr) {
      if (const auto *constant_value_expr =
              dynamic_cast<const ConstantValueExpression *>(comparison_expr->GetChildren()[1].get());
          constant_value_expr != nullptr) {
        bool exist = false;
        int idx = 0;
        for (auto ci : *column_indexes) {
          if (ci == column_value_expr->GetColIdx()) {
            exist = true;
            break;
          }
          idx++;
        }

        if (!exist) {
          // not exists
          idx = static_cast<int>(column_indexes->size());
          column_indexes->push_back(column_value_expr->GetColIdx());
          first_values->push_back(Value(TypeId::INTEGER, 0));
          last_values->push_back(Value(TypeId::INTEGER, 0));
        }

        std::vector<Column> columns;
        auto v = constant_value_expr->Evaluate(nullptr, Schema(columns));
        if (comparison_expr->comp_type_ == ComparisonType::GreaterThan ||
            comparison_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
          if ((*first_values)[idx].CompareLessThan(v) == CmpBool::CmpTrue) {
            (*first_values)[idx] = v;
          }
        }

        if (comparison_expr->comp_type_ == ComparisonType::LessThan ||
            comparison_expr->comp_type_ == ComparisonType::LessThanOrEqual) {
          if ((*last_values)[idx].CompareGreaterThan(v) == CmpBool::CmpTrue) {
            (*last_values)[idx] = v;
          }
        }

        if (comparison_expr->comp_type_ == ComparisonType::Equal) {
          (*first_values)[idx] = v;
          (*last_values)[idx] = v;
        }

        return true;
      }
    }

    return false;
  }

  return false;
}

auto Optimizer::OptimizeSeqscanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // the update case is too complex, so we simply choose not to perform optimization
  if (plan->GetType() == PlanType::Update || plan->GetType() == PlanType::Delete) {
    return plan;
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqscanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

    // get column index, and value expression
    std::vector<uint32_t> column_indexes;
    std::vector<Value> first_values;
    std::vector<Value> last_values;
    if (seq_plan.filter_predicate_ != nullptr &&
        CheckIfColumnConstantConjunction(seq_plan.filter_predicate_, &column_indexes, &first_values, &last_values)) {
      // swicth to a index scan
      const auto *table_info = catalog_.GetTable(seq_plan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);

      for (const auto *index : indices) {
        const auto &columns = index->key_schema_.GetColumns();
        // check index key schema == order by columns
        bool valid = true;
        if (columns.size() == column_indexes.size()) {
          for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i].GetName() != table_info->schema_.GetColumn(column_indexes[i]).GetName()) {
              valid = false;
              break;
            }
          }
          if (valid) {
            if (std::all_of(first_values.begin(), first_values.end(), [](Value &n) {
                  return (n.CompareEquals(Value(TypeId::INTEGER, 0)) == CmpBool::CmpTrue);
                })) {
              return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index->index_oid_,
                                                         seq_plan.filter_predicate_);
            }
            Tuple first_t = Tuple(first_values, &(index->key_schema_));
            IntegerKeyType first_key;
            first_key.SetFromKey(first_t);

            if (std::all_of(last_values.begin(), last_values.end(), [](Value &n) {
                  return (n.CompareEquals(Value(TypeId::INTEGER, 0)) == CmpBool::CmpTrue);
                })) {
              return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index->index_oid_,
                                                         seq_plan.filter_predicate_);
            }
            Tuple last_t = Tuple(last_values, &(index->key_schema_));
            IntegerKeyType last_key;
            last_key.SetFromKey(last_t);
            return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index->index_oid_,
                                                       seq_plan.filter_predicate_, first_key, last_key);
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
