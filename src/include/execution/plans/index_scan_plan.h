//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_plan.h
//
// Identification: src/include/execution/plans/index_scan_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {
/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node.
   * @param output the output format of this scan plan node
   * @param table_oid the identifier of table to be scanned
   */
  IndexScanPlanNode(SchemaRef output, index_oid_t index_oid, AbstractExpressionRef filter_predicate = nullptr,
                    std::optional<IntegerKeyType> first_key = std::nullopt,
                    std::optional<IntegerKeyType> last_key = std::nullopt)
      : AbstractPlanNode(std::move(output), {}),
        index_oid_(index_oid),
        filter_predicate_(std::move(filter_predicate)),
        first_key_(first_key),
        last_key_(last_key) {}

  auto Predicate() const -> const AbstractExpressionRef & { return filter_predicate_; }

  auto GetType() const -> PlanType override { return PlanType::IndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(IndexScanPlanNode);

  /** The table whose tuples should be scanned. */
  index_oid_t index_oid_;

  // Add anything you want here for index lookup

  AbstractExpressionRef filter_predicate_;

  std::optional<IntegerKeyType> first_key_;
  std::optional<IntegerKeyType> last_key_;

 protected:
  auto PlanNodeToString() const -> std::string override {
    return fmt::format("IndexScan {{ index_oid={} }}", index_oid_);
  }
};

}  // namespace bustub
