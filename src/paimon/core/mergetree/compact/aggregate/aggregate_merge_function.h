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
#include <cassert>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class CoreOptions;

/// A `MergeFunction` where key is primary key (unique) and value is the partial record,
/// pre-aggregate non-null fields on merge.
class AggregateMergeFunction : public MergeFunction {
 public:
    // value_schema is the schema of parameter value in KeyValue object
    static Result<std::unique_ptr<AggregateMergeFunction>> Create(
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& primary_keys, const CoreOptions& options);

    void Reset() override {
        latest_kv_ = std::nullopt;
        current_delete_row_ = false;
        row_ = std::make_unique<GenericRow>(getters_.size());
        for (const auto& agg : aggregators_) {
            agg->Reset();
        }
    }

    Status Add(KeyValue&& kv) override;

    Result<std::optional<KeyValue>> GetResult() override;

 private:
    AggregateMergeFunction(std::vector<InternalRow::FieldGetterFunc>&& getters,
                           std::vector<std::unique_ptr<FieldAggregator>>&& aggregators,
                           bool remove_record_on_delete)
        : getters_(std::move(getters)),
          aggregators_(std::move(aggregators)),
          remove_record_on_delete_(remove_record_on_delete),
          row_(std::make_unique<GenericRow>(getters_.size())) {
        assert(getters_.size() == aggregators_.size());
    }
    static Result<std::string> GetAggFuncName(const std::string& field_name,
                                              const std::vector<std::string>& primary_keys,
                                              const CoreOptions& options);

 private:
    std::vector<InternalRow::FieldGetterFunc> getters_;
    std::vector<std::unique_ptr<FieldAggregator>> aggregators_;
    bool remove_record_on_delete_;
    bool current_delete_row_ = false;
    std::optional<KeyValue> latest_kv_;
    std::unique_ptr<GenericRow> row_;
};
}  // namespace paimon
