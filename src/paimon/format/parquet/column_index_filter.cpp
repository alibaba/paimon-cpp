/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/format/parquet/column_index_filter.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <memory>
#include <set>

#include "paimon/data/decimal.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"

namespace paimon::parquet {

Result<RowRanges> ColumnIndexFilter::CalculateRowRanges(
    const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<::parquet::PageIndexReader>& page_index_reader,
    const std::map<std::string, int32_t>& column_name_to_index, int32_t row_group_index,
    int64_t row_group_row_count) {
    if (!predicate || !page_index_reader) {
        return RowRanges::CreateSingle(row_group_row_count);
    }

    auto rg_page_index_reader = page_index_reader->RowGroup(row_group_index);
    if (!rg_page_index_reader) {
        return RowRanges::CreateSingle(row_group_row_count);
    }

    return VisitPredicate(predicate, rg_page_index_reader.get(), column_name_to_index,
                          row_group_row_count);
}

Result<RowRanges> ColumnIndexFilter::VisitPredicate(
    const std::shared_ptr<Predicate>& predicate,
    ::parquet::RowGroupPageIndexReader* rg_page_index_reader,
    const std::map<std::string, int32_t>& column_name_to_index, int64_t row_group_row_count) {
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        return VisitLeafPredicate(leaf_predicate, rg_page_index_reader, column_name_to_index,
                                  row_group_row_count);
    }

    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return VisitCompoundPredicate(compound_predicate, rg_page_index_reader,
                                      column_name_to_index, row_group_row_count);
    }

    return Status::Invalid("Unknown predicate type");
}

Result<RowRanges> ColumnIndexFilter::VisitLeafPredicate(
    const std::shared_ptr<LeafPredicate>& leaf_predicate,
    ::parquet::RowGroupPageIndexReader* rg_page_index_reader,
    const std::map<std::string, int32_t>& column_name_to_index, int64_t row_group_row_count) {
    const std::string& field_name = leaf_predicate->FieldName();
    auto it = column_name_to_index.find(field_name);
    if (it == column_name_to_index.end()) {
        // Column not found in file (schema evolution): all values are treated as NULL.
        // Return precise results based on predicate type, matching Java behavior.
        const auto& function = leaf_predicate->GetFunction();
        auto function_type = function.GetType();
        const auto& literals = leaf_predicate->Literals();
        switch (function_type) {
            case Function::Type::IS_NULL:
                // All values are null, IS_NULL matches all rows.
                return RowRanges::CreateSingle(row_group_row_count);
            case Function::Type::EQUAL: {
                // NULL = null_literal → all rows (null-safe equal semantics);
                // NULL = non_null → no rows.
                bool has_null_literal = !literals.empty() && literals[0].IsNull();
                return has_null_literal ? RowRanges::CreateSingle(row_group_row_count)
                                        : RowRanges::CreateEmpty();
            }
            case Function::Type::IN: {
                // IN list contains null → all rows; otherwise no rows.
                bool has_null = std::any_of(literals.begin(), literals.end(),
                                            [](const Literal& l) { return l.IsNull(); });
                return has_null ? RowRanges::CreateSingle(row_group_row_count)
                                : RowRanges::CreateEmpty();
            }
            case Function::Type::NOT_EQUAL: {
                // NULL != null_literal → no rows; NULL != non_null → all rows
                // (safe over-approximation matching Java).
                bool has_null_literal = !literals.empty() && literals[0].IsNull();
                return has_null_literal ? RowRanges::CreateEmpty()
                                        : RowRanges::CreateSingle(row_group_row_count);
            }
            case Function::Type::NOT_IN: {
                // NOT_IN list contains null → no rows; otherwise all rows
                // (safe over-approximation matching Java).
                bool has_null = std::any_of(literals.begin(), literals.end(),
                                            [](const Literal& l) { return l.IsNull(); });
                return has_null ? RowRanges::CreateEmpty()
                                : RowRanges::CreateSingle(row_group_row_count);
            }
            case Function::Type::IS_NOT_NULL:
            case Function::Type::LESS_THAN:
            case Function::Type::LESS_OR_EQUAL:
            case Function::Type::GREATER_THAN:
            case Function::Type::GREATER_OR_EQUAL:
                // All values are null, these predicates cannot match any row.
                return RowRanges::CreateEmpty();
            default:
                // Unknown predicate type, safe fallback to all rows.
                return RowRanges::CreateSingle(row_group_row_count);
        }
    }

    int32_t column_index = it->second;
    auto column_index_ptr = rg_page_index_reader->GetColumnIndex(column_index);
    auto offset_index_ptr = rg_page_index_reader->GetOffsetIndex(column_index);

    if (!column_index_ptr || !offset_index_ptr) {
        // Column index or offset index not available, return all rows
        return RowRanges::CreateSingle(row_group_row_count);
    }

    const auto& function = leaf_predicate->GetFunction();
    auto function_type = function.GetType();
    const auto& literals = leaf_predicate->Literals();
    FieldType field_type = leaf_predicate->GetFieldType();

    std::vector<int32_t> matching_pages;

    switch (function_type) {
        case Function::Type::IS_NULL:
            matching_pages = FilterPagesByIsNull(column_index_ptr, offset_index_ptr);
            break;
        case Function::Type::IS_NOT_NULL:
            matching_pages = FilterPagesByIsNotNull(column_index_ptr, offset_index_ptr);
            break;
        case Function::Type::EQUAL:
            if (!literals.empty()) {
                matching_pages =
                    FilterPagesByEqual(column_index_ptr, offset_index_ptr, literals[0], field_type);
            }
            break;
        case Function::Type::NOT_EQUAL:
            if (!literals.empty()) {
                matching_pages = FilterPagesByNotEqual(column_index_ptr, offset_index_ptr,
                                                       literals[0], field_type);
            }
            break;
        case Function::Type::LESS_THAN:
            if (!literals.empty()) {
                matching_pages = FilterPagesByLessThan(column_index_ptr, offset_index_ptr,
                                                       literals[0], field_type);
            }
            break;
        case Function::Type::LESS_OR_EQUAL:
            if (!literals.empty()) {
                matching_pages = FilterPagesByLessOrEqual(column_index_ptr, offset_index_ptr,
                                                          literals[0], field_type);
            }
            break;
        case Function::Type::GREATER_THAN:
            if (!literals.empty()) {
                matching_pages = FilterPagesByGreaterThan(column_index_ptr, offset_index_ptr,
                                                          literals[0], field_type);
            }
            break;
        case Function::Type::GREATER_OR_EQUAL:
            if (!literals.empty()) {
                matching_pages = FilterPagesByGreaterOrEqual(column_index_ptr, offset_index_ptr,
                                                             literals[0], field_type);
            }
            break;
        case Function::Type::IN:
            matching_pages =
                FilterPagesByIn(column_index_ptr, offset_index_ptr, literals, field_type);
            break;
        case Function::Type::NOT_IN:
            matching_pages = FilterPagesByNotIn(column_index_ptr, offset_index_ptr, literals);
            break;
        default:
            // Unsupported function type for column index filtering
            return RowRanges::CreateSingle(row_group_row_count);
    }

    return BuildRowRangesFromPageIndices(matching_pages, offset_index_ptr, row_group_row_count);
}

Result<RowRanges> ColumnIndexFilter::VisitCompoundPredicate(
    const std::shared_ptr<CompoundPredicate>& compound_predicate,
    ::parquet::RowGroupPageIndexReader* rg_page_index_reader,
    const std::map<std::string, int32_t>& column_name_to_index, int64_t row_group_row_count) {
    const auto& children = compound_predicate->Children();
    const auto& function = compound_predicate->GetFunction();
    auto function_type = function.GetType();

    if (children.empty()) {
        return RowRanges::CreateSingle(row_group_row_count);
    }

    // Calculate row ranges for first child
    PAIMON_ASSIGN_OR_RAISE(RowRanges result,
                           VisitPredicate(children[0], rg_page_index_reader, column_name_to_index,
                                          row_group_row_count));

    if (function_type == Function::Type::AND) {
        // Short-circuit: if result is empty, no need to continue
        if (result.IsEmpty()) {
            return result;
        }

        for (size_t i = 1; i < children.size(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(RowRanges child_ranges,
                                   VisitPredicate(children[i], rg_page_index_reader,
                                                  column_name_to_index, row_group_row_count));

            result = RowRanges::Intersection(result, child_ranges);

            // Short-circuit: if result is empty, no need to continue
            if (result.IsEmpty()) {
                return result;
            }
        }
    } else if (function_type == Function::Type::OR) {
        // Short-circuit: if result already covers all rows, no need to continue
        if (result.RowCount() == row_group_row_count) {
            return result;
        }

        for (size_t i = 1; i < children.size(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(RowRanges child_ranges,
                                   VisitPredicate(children[i], rg_page_index_reader,
                                                  column_name_to_index, row_group_row_count));

            result = RowRanges::Union(result, child_ranges);

            // Short-circuit: if result already covers all rows, no need to continue
            if (result.RowCount() == row_group_row_count) {
                return result;
            }
        }
    } else {
        return Status::Invalid("Unknown compound predicate type");
    }

    return result;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByEqual(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index, const Literal& literal,
    FieldType field_type) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    const auto& min_values = column_index->encoded_min_values();
    const auto& max_values = column_index->encoded_max_values();
    const auto& null_counts = column_index->null_counts();
    bool has_null_counts = column_index->has_null_counts();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            if (literal.IsNull()) {
                matching_pages.push_back(i);
            }
            continue;
        }

        if (literal.IsNull()) {
            // Page is not all-null but may contain some null values.
            // Include the page if null_counts > 0 or null_counts is unavailable.
            if (has_null_counts && null_counts[i] > 0) {
                matching_pages.push_back(i);
            } else if (!has_null_counts) {
                matching_pages.push_back(i);
            }
            continue;
        }

        if (PageMightContainEqual(min_values[i], max_values[i], literal, field_type)) {
            matching_pages.push_back(i);
        }
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByNotEqual(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index, const Literal& literal,
    FieldType field_type) {
    std::vector<int32_t> matching_pages;

    if (literal.IsNull()) {
        // value != NULL is UNKNOWN for any value. No rows can match.
        return matching_pages;
    }

    const auto& null_pages = column_index->null_pages();
    const auto& min_values = column_index->encoded_min_values();
    const auto& max_values = column_index->encoded_max_values();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            // Null-only pages: NULL != x is NULL (UNKNOWN) in SQL semantics,
            // which evaluates to false. Skip null-only pages for NOT_EQUAL.
            continue;
        }

        // Try to exclude pages where min == max == literal (all non-null values equal literal).
        // NULL != literal is NULL (UNKNOWN) in SQL, so nulls don't produce true either.
        auto cmp_min = CompareEncodedWithLiteral(min_values[i], literal, field_type);
        auto cmp_max = CompareEncodedWithLiteral(max_values[i], literal, field_type);
        if (cmp_min.has_value() && cmp_max.has_value() && *cmp_min == 0 && *cmp_max == 0) {
            // min == max == literal: all non-null values equal literal, and nulls
            // don't satisfy != either. Skip this page entirely.
            continue;
        }

        matching_pages.push_back(i);
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByLessThan(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index, const Literal& literal,
    FieldType field_type) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    const auto& min_values = column_index->encoded_min_values();
    const auto& max_values = column_index->encoded_max_values();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            continue;
        }

        if (PageMightContainLessThan(min_values[i], max_values[i], literal, field_type)) {
            matching_pages.push_back(i);
        }
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByLessOrEqual(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index, const Literal& literal,
    FieldType field_type) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    const auto& min_values = column_index->encoded_min_values();
    const auto& max_values = column_index->encoded_max_values();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            continue;
        }

        if (PageMightContainLessOrEqual(min_values[i], max_values[i], literal, field_type)) {
            matching_pages.push_back(i);
        }
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByGreaterThan(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index, const Literal& literal,
    FieldType field_type) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    const auto& min_values = column_index->encoded_min_values();
    const auto& max_values = column_index->encoded_max_values();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            continue;
        }

        if (PageMightContainGreaterThan(min_values[i], max_values[i], literal, field_type)) {
            matching_pages.push_back(i);
        }
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByGreaterOrEqual(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index, const Literal& literal,
    FieldType field_type) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    const auto& min_values = column_index->encoded_min_values();
    const auto& max_values = column_index->encoded_max_values();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            continue;
        }

        if (PageMightContainGreaterOrEqual(min_values[i], max_values[i], literal, field_type)) {
            matching_pages.push_back(i);
        }
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByIsNull(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    const auto& null_counts = column_index->null_counts();
    bool has_null_counts = column_index->has_null_counts();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            matching_pages.push_back(i);
            continue;
        }

        if (has_null_counts && null_counts[i] > 0) {
            matching_pages.push_back(i);
        } else if (!has_null_counts) {
            matching_pages.push_back(i);
        }
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByIsNotNull(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    for (int32_t i = 0; i < num_pages; ++i) {
        if (!null_pages[i]) {
            matching_pages.push_back(i);
        }
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByIn(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
    const std::vector<Literal>& literals, FieldType field_type) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    const auto& min_values = column_index->encoded_min_values();
    const auto& max_values = column_index->encoded_max_values();
    const auto& null_counts = column_index->null_counts();
    bool has_null_counts = column_index->has_null_counts();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    bool has_null =
        std::any_of(literals.begin(), literals.end(), [](const Literal& l) { return l.IsNull(); });

    // Pages outer loop, literals inner loop with early break when page is matched.
    // Naturally produces sorted output, avoids unordered_set overhead.
    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            // All-null page: include only if IN list contains null
            if (has_null) {
                matching_pages.push_back(i);
            }
            continue;
        }

        // Check null-in-list match for non-all-null pages
        if (has_null) {
            if ((has_null_counts && null_counts[i] > 0) || !has_null_counts) {
                matching_pages.push_back(i);
                continue;  // Already matched, skip literal checks
            }
        }

        // Check non-null literals against page min/max with early break
        for (const auto& literal : literals) {
            if (literal.IsNull()) {
                continue;
            }
            if (PageMightContainEqual(min_values[i], max_values[i], literal, field_type)) {
                matching_pages.push_back(i);
                break;  // Page matched, no need to check more literals
            }
        }
    }

    return matching_pages;
}

std::vector<int32_t> ColumnIndexFilter::FilterPagesByNotIn(
    const std::shared_ptr<::parquet::ColumnIndex>& column_index,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
    const std::vector<Literal>& literals) {
    std::vector<int32_t> matching_pages;
    const auto& null_pages = column_index->null_pages();
    int32_t num_pages = static_cast<int32_t>(null_pages.size());

    bool has_null = false;
    for (const auto& literal : literals) {
        if (literal.IsNull()) {
            has_null = true;
            break;
        }
    }

    if (has_null) {
        // NOT_IN list contains null → value NOT IN (..., NULL, ...) evaluates to
        // UNKNOWN for every value (because it expands to AND(..., value != NULL, ...)
        // and value != NULL is always UNKNOWN). No rows can match.
        return matching_pages;
    }

    for (int32_t i = 0; i < num_pages; ++i) {
        if (null_pages[i]) {
            // Null-only pages: NULL NOT IN (non-null values) is UNKNOWN, skip.
            continue;
        }

        // Non-null pages could contain values not in the list
        matching_pages.push_back(i);
    }

    return matching_pages;
}

RowRanges ColumnIndexFilter::BuildRowRangesFromPageIndices(
    const std::vector<int32_t>& page_indices,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index, int64_t row_group_row_count) {
    if (page_indices.empty()) {
        return RowRanges::CreateEmpty();
    }

    const auto& page_locations = offset_index->page_locations();
    RowRanges ranges;

    for (int32_t page_idx : page_indices) {
        if (page_idx < 0 || page_idx >= static_cast<int32_t>(page_locations.size())) {
            continue;
        }

        int64_t first_row_index = page_locations[page_idx].first_row_index;

        int64_t last_row_index;
        if (page_idx + 1 < static_cast<int32_t>(page_locations.size())) {
            last_row_index = page_locations[page_idx + 1].first_row_index - 1;
        } else {
            last_row_index = row_group_row_count - 1;
        }

        ranges.Add(RowRanges::Range(first_row_index, last_row_index));
    }

    return ranges;
}

std::optional<int32_t> ColumnIndexFilter::CompareEncodedWithLiteral(const std::string& encoded,
                                                                    const Literal& literal,
                                                                    FieldType field_type) {
    if (literal.IsNull()) {
        return std::nullopt;
    }

    switch (field_type) {
        case FieldType::BOOLEAN: {
            if (encoded.size() < 1) return std::nullopt;
            int32_t enc_val = (encoded[0] != 0) ? 1 : 0;
            int32_t lit_val = literal.GetValue<bool>() ? 1 : 0;
            return (enc_val < lit_val) ? -1 : (enc_val > lit_val) ? 1 : 0;
        }
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::DATE: {
            if (encoded.size() < sizeof(int32_t)) return std::nullopt;
            int32_t enc_val;
            std::memcpy(&enc_val, encoded.data(), sizeof(int32_t));
            int32_t lit_val;
            if (field_type == FieldType::TINYINT) {
                lit_val = static_cast<int32_t>(literal.GetValue<int8_t>());
            } else if (field_type == FieldType::SMALLINT) {
                lit_val = static_cast<int32_t>(literal.GetValue<int16_t>());
            } else {
                lit_val = literal.GetValue<int32_t>();
            }
            return (enc_val < lit_val) ? -1 : (enc_val > lit_val) ? 1 : 0;
        }
        case FieldType::BIGINT: {
            if (encoded.size() < sizeof(int64_t)) return std::nullopt;
            int64_t enc_val;
            std::memcpy(&enc_val, encoded.data(), sizeof(int64_t));
            int64_t lit_val = literal.GetValue<int64_t>();
            return (enc_val < lit_val) ? -1 : (enc_val > lit_val) ? 1 : 0;
        }
        case FieldType::FLOAT: {
            if (encoded.size() < sizeof(float)) return std::nullopt;
            float enc_val;
            std::memcpy(&enc_val, encoded.data(), sizeof(float));
            float lit_val = literal.GetValue<float>();
            if (std::isnan(enc_val) || std::isnan(lit_val)) return std::nullopt;
            return (enc_val < lit_val) ? -1 : (enc_val > lit_val) ? 1 : 0;
        }
        case FieldType::DOUBLE: {
            if (encoded.size() < sizeof(double)) return std::nullopt;
            double enc_val;
            std::memcpy(&enc_val, encoded.data(), sizeof(double));
            double lit_val = literal.GetValue<double>();
            if (std::isnan(enc_val) || std::isnan(lit_val)) return std::nullopt;
            return (enc_val < lit_val) ? -1 : (enc_val > lit_val) ? 1 : 0;
        }
        case FieldType::STRING:
        case FieldType::BINARY: {
            std::string lit_val = literal.GetValue<std::string>();
            int cmp = encoded.compare(lit_val);
            return (cmp < 0) ? -1 : (cmp > 0) ? 1 : 0;
        }
        case FieldType::DECIMAL: {
            // Parquet stores DECIMAL as INT32, INT64, or FIXED_LEN_BYTE_ARRAY depending
            // on precision. All are stored as unscaled integer values.
            Decimal lit_decimal = literal.GetValue<Decimal>();
            Decimal::int128_t lit_val = lit_decimal.Value();
            Decimal::int128_t enc_val;

            if (encoded.size() == sizeof(int32_t)) {
                // INT32 physical type (precision <= 9)
                int32_t raw;
                std::memcpy(&raw, encoded.data(), sizeof(int32_t));
                enc_val = static_cast<Decimal::int128_t>(raw);
            } else if (encoded.size() == sizeof(int64_t)) {
                // INT64 physical type (precision <= 18)
                int64_t raw;
                std::memcpy(&raw, encoded.data(), sizeof(int64_t));
                enc_val = static_cast<Decimal::int128_t>(raw);
            } else {
                // FIXED_LEN_BYTE_ARRAY: big-endian two's complement
                if (encoded.empty()) return std::nullopt;
                // Sign-extend from the first byte
                enc_val = (static_cast<int8_t>(encoded[0]) < 0) ? static_cast<Decimal::int128_t>(-1)
                                                                : static_cast<Decimal::int128_t>(0);
                for (size_t i = 0; i < encoded.size(); ++i) {
                    enc_val = (enc_val << 8) | static_cast<uint8_t>(encoded[i]);
                }
            }

            return (enc_val < lit_val) ? -1 : (enc_val > lit_val) ? 1 : 0;
        }
        default:
            // TIMESTAMP, etc. - not yet supported for page-level filtering.
            // TIMESTAMP is blocked at predicate_converter level (returns NotImplemented).
            // Return nullopt to fall back to safe behavior (include page).
            return std::nullopt;
    }
}

bool ColumnIndexFilter::PageMightContainEqual(const std::string& encoded_min,
                                              const std::string& encoded_max,
                                              const Literal& literal, FieldType field_type) {
    if (literal.IsNull()) {
        return false;  // Null is handled separately via null_pages
    }

    // Page might contain equal if min <= literal <= max
    auto cmp_min = CompareEncodedWithLiteral(encoded_min, literal, field_type);
    if (!cmp_min.has_value()) return true;  // Can't compare, assume match
    if (*cmp_min > 0) return false;         // min > literal

    auto cmp_max = CompareEncodedWithLiteral(encoded_max, literal, field_type);
    if (!cmp_max.has_value()) return true;
    if (*cmp_max < 0) return false;  // max < literal

    return true;  // min <= literal <= max
}

bool ColumnIndexFilter::PageMightContainLessThan(const std::string& encoded_min,
                                                 const std::string& encoded_max,
                                                 const Literal& literal, FieldType field_type) {
    if (literal.IsNull()) {
        return false;
    }

    // Page might contain values < literal if min < literal
    auto cmp_min = CompareEncodedWithLiteral(encoded_min, literal, field_type);
    if (!cmp_min.has_value()) return true;
    return *cmp_min < 0;
}

bool ColumnIndexFilter::PageMightContainLessOrEqual(const std::string& encoded_min,
                                                    const std::string& encoded_max,
                                                    const Literal& literal, FieldType field_type) {
    if (literal.IsNull()) {
        return false;
    }

    // Page might contain values <= literal if min <= literal
    auto cmp_min = CompareEncodedWithLiteral(encoded_min, literal, field_type);
    if (!cmp_min.has_value()) return true;
    return *cmp_min <= 0;
}

bool ColumnIndexFilter::PageMightContainGreaterThan(const std::string& encoded_min,
                                                    const std::string& encoded_max,
                                                    const Literal& literal, FieldType field_type) {
    if (literal.IsNull()) {
        return false;
    }

    // Page might contain values > literal if max > literal
    auto cmp_max = CompareEncodedWithLiteral(encoded_max, literal, field_type);
    if (!cmp_max.has_value()) return true;
    return *cmp_max > 0;
}

bool ColumnIndexFilter::PageMightContainGreaterOrEqual(const std::string& encoded_min,
                                                       const std::string& encoded_max,
                                                       const Literal& literal,
                                                       FieldType field_type) {
    if (literal.IsNull()) {
        return false;
    }

    // Page might contain values >= literal if max >= literal
    auto cmp_max = CompareEncodedWithLiteral(encoded_max, literal, field_type);
    if (!cmp_max.has_value()) return true;
    return *cmp_max >= 0;
}

}  // namespace paimon::parquet
