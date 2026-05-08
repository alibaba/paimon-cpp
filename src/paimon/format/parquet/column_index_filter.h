/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/defs.h"
#include "paimon/format/parquet/row_ranges.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "parquet/page_index.h"

namespace paimon {
class CompoundPredicate;
class LeafPredicate;
class Literal;
}  // namespace paimon

namespace paimon::parquet {

/// ColumnIndexFilter calculates row ranges based on ColumnIndex statistics.
/// It uses the min/max values in the column index to determine which pages
/// might contain rows matching the predicate.
///
/// The computed RowRanges serve two purposes:
/// 1. Row-group elimination: if no pages match, the entire row group is skipped.
/// 2. Page-level skipping: for partially matched row groups, RowRanges are passed
///    to PageFilteredRowGroupReader which uses data_page_filter to skip
///    non-matching pages at the I/O level, and SkipRecords/ReadRecords to skip
///    non-matching rows at the decode level within kept pages.
class ColumnIndexFilter {
 public:
    ColumnIndexFilter() = delete;

    /// Calculate row ranges based on predicate and column indices.
    /// @param predicate The predicate to evaluate.
    /// @param page_index_reader The page index reader for the file.
    /// @param column_name_to_index Map from column name to column index.
    /// @param row_group_index The row group index to filter.
    /// @param row_group_row_count The number of rows in the row group.
    /// @return RowRanges that may contain matching rows.
    static Result<RowRanges> CalculateRowRanges(
        const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<::parquet::PageIndexReader>& page_index_reader,
        const std::map<std::string, int32_t>& column_name_to_index, int32_t row_group_index,
        int64_t row_group_row_count);

 private:
    /// Visit a predicate and calculate row ranges.
    static Result<RowRanges> VisitPredicate(
        const std::shared_ptr<Predicate>& predicate,
        const std::map<std::string, int32_t>& column_name_to_index, int64_t row_group_row_count,
        ::parquet::RowGroupPageIndexReader* rg_page_index_reader);

    /// Visit a leaf predicate and calculate row ranges.
    static Result<RowRanges> VisitLeafPredicate(
        const std::shared_ptr<LeafPredicate>& leaf_predicate,
        const std::map<std::string, int32_t>& column_name_to_index, int64_t row_group_row_count,
        ::parquet::RowGroupPageIndexReader* rg_page_index_reader);

    /// Visit a compound predicate (AND/OR) and calculate row ranges.
    static Result<RowRanges> VisitCompoundPredicate(
        const std::shared_ptr<CompoundPredicate>& compound_predicate,
        const std::map<std::string, int32_t>& column_name_to_index, int64_t row_group_row_count,
        ::parquet::RowGroupPageIndexReader* rg_page_index_reader);

    /// Filter pages based on column index statistics for EQUAL predicate.
    static std::vector<int32_t> FilterPagesByEqual(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index, const Literal& literal,
        FieldType field_type);

    /// Filter pages based on column index statistics for NOT_EQUAL predicate.
    static std::vector<int32_t> FilterPagesByNotEqual(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index, const Literal& literal,
        FieldType field_type);

    /// Filter pages based on column index statistics for LESS_THAN predicate.
    static std::vector<int32_t> FilterPagesByLessThan(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index, const Literal& literal,
        FieldType field_type);

    /// Filter pages based on column index statistics for LESS_OR_EQUAL predicate.
    static std::vector<int32_t> FilterPagesByLessOrEqual(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index, const Literal& literal,
        FieldType field_type);

    /// Filter pages based on column index statistics for GREATER_THAN predicate.
    static std::vector<int32_t> FilterPagesByGreaterThan(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index, const Literal& literal,
        FieldType field_type);

    /// Filter pages based on column index statistics for GREATER_OR_EQUAL predicate.
    static std::vector<int32_t> FilterPagesByGreaterOrEqual(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index, const Literal& literal,
        FieldType field_type);

    /// Filter pages based on column index statistics for IS_NULL predicate.
    static std::vector<int32_t> FilterPagesByIsNull(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index);

    /// Filter pages based on column index statistics for IS_NOT_NULL predicate.
    static std::vector<int32_t> FilterPagesByIsNotNull(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index);

    /// Filter pages based on column index statistics for IN predicate.
    static std::vector<int32_t> FilterPagesByIn(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index,
        const std::vector<Literal>& literals, FieldType field_type);

    /// Filter pages based on column index statistics for NOT_IN predicate.
    static std::vector<int32_t> FilterPagesByNotIn(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index,
        const std::vector<Literal>& literals);

    /// Build row ranges from page indices (must be sorted in ascending order).
    static RowRanges BuildRowRangesFromPageIndices(
        const std::vector<int32_t>& page_indices,
        const std::shared_ptr<::parquet::OffsetIndex>& offset_index, int64_t row_group_row_count);

    /// Compare a parquet encoded value with a Literal.
    /// @return -1 if encoded < literal, 0 if equal, 1 if encoded > literal.
    ///         nullopt if comparison cannot be performed (unsupported type, etc.).
    static std::optional<int32_t> CompareEncodedWithLiteral(const std::string& encoded,
                                                            const Literal& literal,
                                                            FieldType field_type);

    /// Check if a page might contain a value equal to the literal.
    /// Condition: min <= literal <= max
    static bool PageMightContainEqual(const std::string& encoded_min,
                                      const std::string& encoded_max, const Literal& literal,
                                      FieldType field_type);

    /// Check if a page might contain values less than the literal.
    /// Condition: min < literal
    static bool PageMightContainLessThan(const std::string& encoded_min, const Literal& literal,
                                         FieldType field_type);

    /// Check if a page might contain values less than or equal to the literal.
    /// Condition: min <= literal
    static bool PageMightContainLessOrEqual(const std::string& encoded_min, const Literal& literal,
                                            FieldType field_type);

    /// Check if a page might contain values greater than the literal.
    /// Condition: max > literal
    static bool PageMightContainGreaterThan(const std::string& encoded_max, const Literal& literal,
                                            FieldType field_type);

    /// Check if a page might contain values greater than or equal to the literal.
    /// Condition: max >= literal
    static bool PageMightContainGreaterOrEqual(const std::string& encoded_max,
                                               const Literal& literal, FieldType field_type);
};

}  // namespace paimon::parquet
