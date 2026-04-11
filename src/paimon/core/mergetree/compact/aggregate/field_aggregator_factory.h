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

#pragma once
#include <memory>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"
#include "paimon/core/mergetree/compact/aggregate/field_bool_and_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_bool_or_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_first_non_null_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_first_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_ignore_retract_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_last_non_null_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_last_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_listagg_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_max_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_min_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_primary_key_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_sum_agg.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
/// %Factory for `FieldAggregator`.
class FieldAggregatorFactory {
 public:
    FieldAggregatorFactory() = delete;
    ~FieldAggregatorFactory() = delete;

    static Result<std::unique_ptr<FieldAggregator>> CreateFieldAggregator(
        const std::string& field_name, const std::shared_ptr<arrow::DataType>& field_type,
        const std::string& str_agg, const CoreOptions& options) {
        std::unique_ptr<FieldAggregator> field_aggregator;
        if (str_agg == FieldPrimaryKeyAgg::NAME) {
            field_aggregator = std::make_unique<FieldPrimaryKeyAgg>(field_type);
        } else if (str_agg == FieldLastNonNullValueAgg::NAME) {
            field_aggregator = std::make_unique<FieldLastNonNullValueAgg>(field_type);
        } else if (str_agg == FieldFirstNonNullValueAgg::NAME) {
            field_aggregator = std::make_unique<FieldFirstNonNullValueAgg>(field_type);
        } else if (str_agg == FieldLastValueAgg::NAME) {
            field_aggregator = std::make_unique<FieldLastValueAgg>(field_type);
        } else if (str_agg == FieldFirstValueAgg::NAME) {
            field_aggregator = std::make_unique<FieldFirstValueAgg>(field_type);
        } else if (str_agg == FieldSumAgg::NAME) {
            PAIMON_ASSIGN_OR_RAISE(field_aggregator, FieldSumAgg::Create(field_type));
        } else if (str_agg == FieldMinAgg::NAME) {
            PAIMON_ASSIGN_OR_RAISE(field_aggregator, FieldMinAgg::Create(field_type));
        } else if (str_agg == FieldMaxAgg::NAME) {
            PAIMON_ASSIGN_OR_RAISE(field_aggregator, FieldMaxAgg::Create(field_type));
        } else if (str_agg == FieldBoolOrAgg::NAME) {
            PAIMON_ASSIGN_OR_RAISE(field_aggregator, FieldBoolOrAgg::Create(field_type));
        } else if (str_agg == FieldBoolAndAgg::NAME) {
            PAIMON_ASSIGN_OR_RAISE(field_aggregator, FieldBoolAndAgg::Create(field_type));
        } else if (str_agg == FieldListaggAgg::NAME) {
            PAIMON_ASSIGN_OR_RAISE(field_aggregator,
                                   FieldListaggAgg::Create(field_type, options, field_name));
        } else {
            return Status::Invalid(fmt::format(
                "Use unsupported aggregation {} or spell aggregate function incorrectly!",
                str_agg));
        }
        PAIMON_ASSIGN_OR_RAISE(bool ignore_retract, options.FieldAggIgnoreRetract(field_name));
        if (ignore_retract) {
            field_aggregator = std::make_unique<FieldIgnoreRetractAgg>(std::move(field_aggregator));
        }
        return field_aggregator;
    }
};
}  // namespace paimon
