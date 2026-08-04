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

#include "paimon/core/mergetree/compact/aggregate/field_merge_map_agg.h"

#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/generic_map.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregate_utils.h"
#include "paimon/status.h"

namespace paimon {
namespace {

struct MapEntry {
    VariantType key;
    VariantType value;
};

Result<int32_t> FindKey(const std::vector<MapEntry>& entries, const VariantType& key,
                        const std::shared_ptr<arrow::DataType>& key_type) {
    for (int32_t i = 0; i < static_cast<int32_t>(entries.size()); ++i) {
        PAIMON_ASSIGN_OR_RAISE(bool equal,
                               FieldAggregateUtils::Equals(entries[i].key, key, key_type));
        if (equal) {
            return i;
        }
    }
    return -1;
}

Status PutMap(const std::shared_ptr<InternalMap>& map,
              const std::shared_ptr<arrow::DataType>& key_type,
              const std::shared_ptr<arrow::DataType>& value_type, std::vector<MapEntry>* entries) {
    std::shared_ptr<InternalArray> keys = map->KeyArray();
    std::shared_ptr<InternalArray> values = map->ValueArray();
    for (int32_t i = 0; i < map->Size(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(VariantType key, FieldAggregateUtils::GetValue(*keys, i, key_type));
        PAIMON_ASSIGN_OR_RAISE(VariantType value,
                               FieldAggregateUtils::GetValue(*values, i, value_type));
        PAIMON_ASSIGN_OR_RAISE(int32_t existing, FindKey(*entries, key, key_type));
        if (existing >= 0) {
            (*entries)[existing].value = std::move(value);
        } else {
            entries->push_back(MapEntry{std::move(key), std::move(value)});
        }
    }
    return Status::OK();
}

VariantType MakeMap(std::vector<MapEntry> entries,
                    std::vector<std::shared_ptr<InternalArray>> key_holders,
                    std::vector<std::shared_ptr<InternalArray>> value_holders) {
    std::vector<VariantType> keys;
    std::vector<VariantType> values;
    keys.reserve(entries.size());
    values.reserve(entries.size());
    for (MapEntry& entry : entries) {
        keys.push_back(std::move(entry.key));
        values.push_back(std::move(entry.value));
    }
    std::shared_ptr<InternalArray> key_array =
        std::make_shared<GenericArray>(std::move(keys), std::move(key_holders));
    std::shared_ptr<InternalArray> value_array =
        std::make_shared<GenericArray>(std::move(values), std::move(value_holders));
    return std::static_pointer_cast<InternalMap>(
        std::make_shared<GenericMap>(std::move(key_array), std::move(value_array)));
}

}  // namespace

Result<std::unique_ptr<FieldMergeMapAgg>> FieldMergeMapAgg::Create(
    const std::shared_ptr<arrow::DataType>& field_type, const std::string& field_name,
    const std::shared_ptr<MemoryPool>& pool) {
    if (field_type->id() != arrow::Type::MAP) {
        return Status::Invalid(
            fmt::format("invalid field type {} for field '{}' of {}, supposed to be map",
                        field_type->ToString(), field_name, NAME));
    }
    std::shared_ptr<arrow::MapType> map_type =
        arrow::internal::checked_pointer_cast<arrow::MapType>(field_type);
    return std::unique_ptr<FieldMergeMapAgg>(
        new FieldMergeMapAgg(field_type, map_type->key_type(), map_type->item_type(), pool));
}

Result<VariantType> FieldMergeMapAgg::Agg(const VariantType& accumulator,
                                          const VariantType& input_field) {
    return AggImpl(accumulator, input_field);
}

Result<VariantType> FieldMergeMapAgg::AggImpl(const VariantType& accumulator,
                                              const VariantType& input_field) const {
    bool accumulator_null = DataDefine::IsVariantNull(accumulator);
    bool input_null = DataDefine::IsVariantNull(input_field);
    if (accumulator_null || input_null) {
        return accumulator_null ? input_field : accumulator;
    }
    auto accumulator_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(accumulator);
    auto input_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(input_field);
    std::vector<MapEntry> entries;
    entries.reserve(accumulator_map->Size() + input_map->Size());
    PAIMON_RETURN_NOT_OK(PutMap(accumulator_map, key_type_, value_type_, &entries));
    PAIMON_RETURN_NOT_OK(PutMap(input_map, key_type_, value_type_, &entries));
    return MakeMap(std::move(entries), {accumulator_map->KeyArray(), input_map->KeyArray()},
                   {accumulator_map->ValueArray(), input_map->ValueArray()});
}

Result<VariantType> FieldMergeMapAgg::Retract(const VariantType& accumulator,
                                              const VariantType& input_field) const {
    if (DataDefine::IsVariantNull(accumulator) || DataDefine::IsVariantNull(input_field)) {
        return accumulator;
    }
    auto accumulator_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(accumulator);
    auto retract_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(input_field);
    if (retract_map->Size() == 0) {
        return accumulator;
    }

    std::vector<MapEntry> entries;
    entries.reserve(accumulator_map->Size());
    PAIMON_RETURN_NOT_OK(PutMap(accumulator_map, key_type_, value_type_, &entries));
    std::shared_ptr<InternalArray> retract_keys = retract_map->KeyArray();
    for (int32_t i = 0; i < retract_map->Size(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(VariantType key,
                               FieldAggregateUtils::GetValue(*retract_keys, i, key_type_));
        PAIMON_ASSIGN_OR_RAISE(int32_t existing, FindKey(entries, key, key_type_));
        if (existing >= 0) {
            entries.erase(entries.begin() + existing);
        }
    }
    return MakeMap(std::move(entries), {accumulator_map->KeyArray()},
                   {accumulator_map->ValueArray()});
}

}  // namespace paimon
