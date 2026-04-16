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

#include <memory>
#include <string>
#include <unordered_set>

#include "paimon/common/data/data_define.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
/// listagg aggregate a field of a row.
/// Concatenates string values with a delimiter.
class FieldListaggAgg : public FieldAggregator {
 public:
    static constexpr char NAME[] = "listagg";

    static Result<std::unique_ptr<FieldListaggAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type, const CoreOptions& options,
        const std::string& field_name) {
        if (field_type->id() != arrow::Type::type::STRING) {
            return Status::Invalid(
                fmt::format("invalid field type {} for field '{}' of {}, supposed to be string",
                            field_type->ToString(), field_name, NAME));
        }
        PAIMON_ASSIGN_OR_RAISE(std::string delimiter, options.FieldListAggDelimiter(field_name));
        PAIMON_ASSIGN_OR_RAISE(bool distinct, options.FieldCollectAggDistinct(field_name));
        // When delimiter is empty and distinct is true, fall back to whitespace split.
        if (distinct && delimiter.empty()) {
            delimiter = " ";
        }
        return std::unique_ptr<FieldListaggAgg>(
            new FieldListaggAgg(field_type, std::move(delimiter), distinct));
    }

    VariantType Agg(const VariantType& accumulator, const VariantType& input_field) override {
        bool accumulator_null = DataDefine::IsVariantNull(accumulator);
        bool input_null = DataDefine::IsVariantNull(input_field);
        if (accumulator_null || input_null) {
            return accumulator_null ? input_field : accumulator;
        }
        std::string_view acc_str = DataDefine::GetStringView(accumulator);
        std::string_view in_str = DataDefine::GetStringView(input_field);
        if (in_str.empty()) {
            return accumulator;
        }
        if (acc_str.empty()) {
            return input_field;
        }

        if (distinct_) {
            result_ = AggDistinctImpl(acc_str, in_str);
        } else {
            // Build into a local string to avoid aliasing when acc_str points into result_
            std::string new_result;
            new_result.reserve(acc_str.size() + delimiter_.size() + in_str.size());
            new_result.append(acc_str);
            new_result.append(delimiter_);
            new_result.append(in_str);
            result_ = std::move(new_result);
        }
        return std::string_view(result_);
    }

 private:
    std::string AggDistinctImpl(std::string_view acc_str, std::string_view in_str) const {
        // Split accumulator tokens into a set for dedup
        std::unordered_set<std::string_view> seen;
        std::string_view remaining = acc_str;
        while (true) {
            size_t pos = remaining.find(delimiter_);
            std::string_view token =
                (pos == std::string_view::npos) ? remaining : remaining.substr(0, pos);
            if (!token.empty()) {
                seen.insert(token);
            }
            if (pos == std::string_view::npos) {
                break;
            }
            remaining = remaining.substr(pos + delimiter_.size());
        }

        // Start with the full accumulator, then append delimiter + new distinct tokens from input
        std::string result;
        result.reserve(acc_str.size() + in_str.size());
        result.append(acc_str);
        remaining = in_str;
        while (true) {
            size_t pos = remaining.find(delimiter_);
            std::string_view token =
                (pos == std::string_view::npos) ? remaining : remaining.substr(0, pos);
            if (!token.empty() && seen.insert(token).second) {
                result.append(delimiter_);
                result.append(token);
            }
            if (pos == std::string_view::npos) {
                break;
            }
            remaining = remaining.substr(pos + delimiter_.size());
        }
        return result;
    }

    explicit FieldListaggAgg(const std::shared_ptr<arrow::DataType>& field_type,
                             std::string delimiter, bool distinct)
        : FieldAggregator(std::string(NAME), field_type),
          delimiter_(std::move(delimiter)),
          distinct_(distinct) {}

    std::string delimiter_;
    bool distinct_;
    std::string result_;
};
}  // namespace paimon
