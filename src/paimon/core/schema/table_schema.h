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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/jsonizable.h"
#include "paimon/result.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

struct ArrowSchema;

namespace paimon {
/// Schema of a table, including schemaId and fieldId.
class TableSchema : public Jsonizable<TableSchema> {
 public:
    static constexpr int64_t FIRST_SCHEMA_ID = 0;
    static constexpr int32_t PAIMON_07_VERSION = 1;
    static constexpr int32_t PAIMON_08_VERSION = 2;
    static constexpr int32_t CURRENT_VERSION = 3;

    static Result<std::unique_ptr<TableSchema>> Create(
        int64_t schema_id, const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::string>& partition_keys,
        const std::vector<std::string>& primary_keys,
        const std::map<std::string, std::string>& options);

    static Result<std::unique_ptr<TableSchema>> CreateFromJson(const std::string& json_str);

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;

    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    bool operator==(const TableSchema& other) const;

    std::vector<std::string> FieldNames() const;
    int64_t Id() const {
        return id_;
    }
    const std::vector<std::string>& PrimaryKeys() const {
        return primary_keys_;
    }
    const std::vector<std::string>& PartitionKeys() const {
        return partition_keys_;
    }

    const std::vector<std::string>& BucketKeys() const {
        return bucket_keys_;
    }

    int32_t NumBuckets() const {
        return num_bucket_;
    }
    int32_t HighestFieldId() const {
        return highest_field_id_;
    }
    const std::map<std::string, std::string>& Options() const {
        return options_;
    }
    const std::vector<DataField>& Fields() const {
        return fields_;
    }
    Result<std::vector<DataField>> TrimmedPrimaryKeyFields() const;

    Result<DataField> GetField(const std::string& field_name) const;

    Result<DataField> GetField(int32_t field_id) const;

    Result<std::vector<std::string>> TrimmedPrimaryKeys() const;

    std::optional<std::string> Comment() const {
        return comment_;
    }

    bool CrossPartitionUpdate() const;

    Result<std::unique_ptr<::ArrowSchema>> GetArrowSchema() const;

 private:
    JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(TableSchema);

    static Result<std::unique_ptr<TableSchema>> InitSchema(
        int64_t schema_id, const std::vector<DataField>& fields, int32_t highest_field_id,
        const std::vector<std::string>& partition_keys,
        const std::vector<std::string>& primary_keys,
        const std::map<std::string, std::string>& options, int64_t time_millis);

    TableSchema(int32_t version, int64_t id, const std::vector<DataField>& fields,
                int32_t highest_field_id, const std::vector<std::string>& partition_keys,
                const std::vector<std::string>& primary_keys,
                const std::map<std::string, std::string>& options, int64_t time_millis);

    Result<std::vector<std::string>> OriginalBucketKeys() const;

    static Result<std::shared_ptr<arrow::Field>> AssignFieldIdsRecursively(
        const std::shared_ptr<arrow::Field>& field, bool set_field_id, int32_t* field_id);

    static Result<std::shared_ptr<arrow::KeyValueMetadata>> MakeMetaDataWithFieldId(
        const std::shared_ptr<arrow::Field>& field, int32_t field_id);

 private:
    // version of schema for paimon
    int32_t version_ = -1;
    int64_t id_ = -1;
    std::vector<DataField> fields_;
    /// Not available from fields, as some fields may have been deleted.
    int32_t highest_field_id_ = -1;
    std::vector<std::string> partition_keys_;
    std::vector<std::string> primary_keys_;
    std::vector<std::string> bucket_keys_;
    int32_t num_bucket_;
    std::map<std::string, std::string> options_;
    std::optional<std::string> comment_;
    int64_t time_millis_ = -1;
};
}  // namespace paimon
