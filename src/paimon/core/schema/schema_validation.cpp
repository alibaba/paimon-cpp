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

#include "paimon/core/schema/schema_validation.h"

#include <algorithm>
#include <cassert>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "arrow/type.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/preconditions.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/options/changelog_producer.h"
#include "paimon/core/options/expire_config.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/defs.h"
#include "paimon/result.h"

namespace paimon {

bool SchemaValidation::IsComplexType(const std::shared_ptr<arrow::Field>& field) {
    return (field->type()->id() == arrow::Type::TIMESTAMP ||
            field->type()->id() == arrow::Type::DECIMAL || BlobUtils::IsBlobField(field));
}

Status SchemaValidation::ValidateTableSchema(const TableSchema& schema) {
    const auto& field_names = schema.FieldNames();
    PAIMON_RETURN_NOT_OK(ValidateNoDuplicateField(schema.BucketKeys(), "bucket key"));
    PAIMON_RETURN_NOT_OK(ValidateNoDuplicateField(schema.PrimaryKeys(), "primary key"));
    PAIMON_RETURN_NOT_OK(ValidateNoDuplicateField(schema.PartitionKeys(), "partition key"));
    PAIMON_RETURN_NOT_OK(
        Preconditions::CheckState(ObjectUtils::ContainsAll(field_names, schema.PartitionKeys()),
                                  "Table column {} should include all partition fields {}",
                                  field_names, schema.PartitionKeys()));
    PAIMON_RETURN_NOT_OK(
        Preconditions::CheckState(ObjectUtils::ContainsAll(field_names, schema.PrimaryKeys()),
                                  "Table column {} should include all primary key constraint {}",
                                  field_names, schema.PrimaryKeys()));

    PAIMON_RETURN_NOT_OK(
        ValidateOnlyContainPrimitiveType(schema.Fields(), schema.PrimaryKeys(), "primary key"));
    PAIMON_RETURN_NOT_OK(
        ValidateOnlyContainPrimitiveType(schema.Fields(), schema.PartitionKeys(), "partition"));
    // TODO(lisizhuo.lsz): C++ Paimon do not support timestamp & decimal type in partition keys for
    // now.
    PAIMON_RETURN_NOT_OK(ValidateNotContainComplexType(schema.Fields(), schema.PartitionKeys()));

    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(schema.Options()));
    PAIMON_RETURN_NOT_OK(ValidateBucket(schema, options));
    // PAIMON_RETURN_NOT_OK(ValidateDefaultValues(schema));
    // PAIMON_RETURN_NOT_OK(ValidateStartupMode(options));
    PAIMON_RETURN_NOT_OK(ValidateFieldsPrefix(schema, options));
    PAIMON_RETURN_NOT_OK(ValidateSequenceField(schema, options));
    PAIMON_RETURN_NOT_OK(ValidateSequenceGroup(schema, options));

    ChangelogProducer changelog_producer = options.GetChangelogProducer();
    if (schema.PrimaryKeys().empty() && changelog_producer != ChangelogProducer::NONE) {
        return Status::Invalid(
            fmt::format("Can not set {} on table without primary keys, please define primary keys.",
                        Options::CHANGELOG_PRODUCER));
    }
    PAIMON_RETURN_NOT_OK(ValidateChangelogProducer(options));
    PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
        options.GetExpireConfig().GetSnapshotRetainMin() > 0,
        std::string(Options::SNAPSHOT_NUM_RETAINED_MIN) + " should be at least 1"));
    PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
        options.GetExpireConfig().GetSnapshotRetainMin() <=
            options.GetExpireConfig().GetSnapshotRetainMax(),
        std::string(Options::SNAPSHOT_NUM_RETAINED_MIN) + " should not be larger than " +
            std::string(Options::SNAPSHOT_NUM_RETAINED_MAX)));

    // TODO(yonghao.fyh): check changelog num retain
    // TODO(yonghao.fyh): support file format validate data fields
    for (const auto& field_name : field_names) {
        if (SpecialFields::IsSpecialFieldName(field_name)) {
            return Status::Invalid(
                fmt::format("field name '{}' in schema cannot be special field.", field_name));
        }
        if (StringUtils::StartsWith(field_name, SpecialFields::KEY_FIELD_PREFIX)) {
            return Status::Invalid(fmt::format("field name '{}' in schema cannot start with '{}'.",
                                               field_name, SpecialFields::KEY_FIELD_PREFIX));
        }
    }
    // TODO(yonghao.fyh): check streaming read overwrite
    // TODO(yonghao.fyh): check 'partition.expiration-time'
    // TODO(yonghao.fyh): check 'rowkind.field'
    if (options.DeletionVectorsEnabled()) {
        PAIMON_RETURN_NOT_OK(ValidateForDeletionVectors(options));
    }

    PAIMON_RETURN_NOT_OK(ValidateRowTracking(schema, options));
    return Status::OK();
}

Status SchemaValidation::ValidateNoDuplicateField(const std::vector<std::string>& field_names,
                                                  const std::string& error_message_intro) {
    auto duplicate_field_names = ObjectUtils::DuplicateItems(field_names);
    PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
        duplicate_field_names.empty(),
        fmt::format("{} [{}] must not contain duplicate fields. Found: [{}]", error_message_intro,
                    fmt::join(field_names, ", "), fmt::join(duplicate_field_names, ", "))));
    return Status::OK();
}

Status SchemaValidation::ValidateOnlyContainPrimitiveType(
    const std::vector<DataField>& fields, const std::vector<std::string>& field_names,
    const std::string& error_message_intro) {
    if (field_names.empty()) {
        return Status::OK();
    }
    std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> fields_map;
    for (const auto& field : fields) {
        fields_map[field.Name()] = field.Type();
    }
    for (const auto& field_name : field_names) {
        auto it = fields_map.find(field_name);
        if (it != fields_map.end()) {
            auto data_type = it->second;
            if (ArrowSchemaValidator::IsNestedType(data_type)) {
                return Status::Invalid(fmt::format("The type {} in {} field {} is unsupported",
                                                   data_type->ToString(), error_message_intro,
                                                   it->first));
            }
        } else {
            assert(false);
            return Status::Invalid(
                fmt::format("unexpected error, field {} not found in fields map", field_name));
        }
    }
    return Status::OK();
}

Status SchemaValidation::ValidateNotContainComplexType(
    const std::vector<DataField>& fields, const std::vector<std::string>& field_names) {
    if (field_names.empty()) {
        return Status::OK();
    }
    std::unordered_map<std::string, std::shared_ptr<arrow::Field>> fields_map;
    for (const auto& field : fields) {
        fields_map[field.Name()] = field.ArrowField();
    }
    for (const auto& field_name : field_names) {
        auto it = fields_map.find(field_name);
        if (it != fields_map.end()) {
            auto field = it->second;
            if (IsComplexType(field)) {
                return Status::Invalid(
                    fmt::format("The field {} in partition field {} is unsupported",
                                field->ToString(), it->first));
            }
        } else {
            assert(false);
            return Status::Invalid(fmt::format(
                "unexpected error, partition field {} not found in schema", field_name));
        }
    }
    return Status::OK();
}

bool SchemaValidation::IsPostponeBucketTable(const TableSchema& schema, int32_t bucket) {
    return !schema.PrimaryKeys().empty() && bucket == BucketModeDefine::POSTPONE_BUCKET;
}

Status SchemaValidation::ValidateBucket(const TableSchema& schema, const CoreOptions& options) {
    int32_t bucket = options.GetBucket();
    if (bucket == -1) {
        if (options.ToMap().count(Options::BUCKET_KEY)) {
            return Status::Invalid(
                fmt::format("Cannot define '{}' with bucket -1, please specify a bucket number.",
                            Options::BUCKET_KEY));
        }
        if (schema.PrimaryKeys().empty() &&
            options.ToMap().count("full-compaction.delta-commits")) {
            return Status::Invalid(
                "AppendOnlyTable of unware or dynamic bucket does not support "
                "'full-compaction.delta-commits'");
        }
    } else if (bucket < 1 && !IsPostponeBucketTable(schema, bucket)) {
        return Status::Invalid("The number of buckets needs to be greater than 0.");
    } else {
        if (schema.CrossPartitionUpdate()) {
            return Status::Invalid(fmt::format(
                "You should use dynamic bucket (bucket = -1) mode in cross partition update case "
                "(Primary key constraint '{}' not include all partition fields '{}').",
                fmt::join(schema.PrimaryKeys(), ", "), fmt::join(schema.PartitionKeys(), ", ")));
        }
        if (schema.PrimaryKeys().empty() && schema.BucketKeys().empty()) {
            return Status::Invalid("You should define a 'bucket-key' for bucketed append mode.");
        }
        if (!schema.BucketKeys().empty()) {
            std::vector<std::string> bucket_keys = schema.BucketKeys();
            std::vector<std::string> nested_fields;

            for (const auto& field : schema.Fields()) {
                if (std::find(bucket_keys.begin(), bucket_keys.end(), field.Name()) !=
                        bucket_keys.end() &&
                    ArrowSchemaValidator::IsNestedType(field.Type())) {
                    nested_fields.push_back(field.Name());
                }
            }

            if (!nested_fields.empty()) {
                return Status::Invalid(fmt::format(
                    "Nested type cannot be in bucket-key, in your table these keys are: {}",
                    fmt::join(nested_fields, ", ")));
            }
        }
    }
    return Status::OK();
}

Status SchemaValidation::ValidateChangelogProducer(const CoreOptions& options) {
    return Preconditions::CheckState(options.GetChangelogProducer() == ChangelogProducer::NONE,
                                     "C++ Paimon does not support changelog-producer yet. Please "
                                     "keep changelog-producer as 'none'.");
}

Status SchemaValidation::ValidateForDeletionVectors(const CoreOptions& options) {
    PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
        options.GetChangelogProducer() == ChangelogProducer::NONE ||
            options.GetChangelogProducer() == ChangelogProducer::INPUT ||
            options.GetChangelogProducer() == ChangelogProducer::LOOKUP,
        "Deletion vectors mode is only supported for NONE/INPUT/LOOKUP changelog producer now."));
    return Preconditions::CheckState(
        options.GetMergeEngine() != MergeEngine::FIRST_ROW,
        "First row merge engine does not need deletion vectors because there is "
        "no deletion of old data in this merge engine.");
}

Status SchemaValidation::ValidateSequenceGroup(const TableSchema& schema,
                                               const CoreOptions& options) {
    std::unordered_map<std::string, std::set<std::string>> fields2_group;
    auto sequence_groups_map = options.GetFieldsSequenceGroups();
    const std::vector<std::string>& field_names = schema.FieldNames();
    for (const auto& [k, v] : sequence_groups_map) {
        std::vector<std::string> sequence_field_names =
            StringUtils::Split(k, Options::FIELDS_SEPARATOR);
        for (const auto& sequence_field_name : sequence_field_names) {
            if (std::find(field_names.begin(), field_names.end(), sequence_field_name) ==
                field_names.end()) {
                assert(false);
                return Status::Invalid(
                    fmt::format("The sequence field group: {} can not be found in table schema.",
                                sequence_field_name));
            }
        }

        for (const auto& field : StringUtils::Split(v, Options::FIELDS_SEPARATOR)) {
            if (std::find(field_names.begin(), field_names.end(), field) == field_names.end()) {
                return Status::Invalid(
                    fmt::format("Field {} can not be found in table schema.", field));
            }

            if (fields2_group.count(field)) {
                std::vector<std::vector<std::string>> sequence_groups;
                sequence_groups.emplace_back(fields2_group[field].begin(),
                                             fields2_group[field].end());
                sequence_groups.push_back(sequence_field_names);

                std::ostringstream sequence_groups_msg;
                for (const auto& group : sequence_groups) {
                    sequence_groups_msg << "{";
                    for (const auto& group_field : group) {
                        sequence_groups_msg << group_field << " ";
                    }
                    sequence_groups_msg << "} ";
                }
                return Status::Invalid(
                    fmt::format("Field {} is defined repeatedly by multiple groups: {}.", field,
                                sequence_groups_msg.str()));
            }
            fields2_group[field].insert(sequence_field_names.begin(), sequence_field_names.end());
        }
    }

    std::set<std::string> illegal_group;
    for (const auto& group : fields2_group) {
        for (const auto& field : group.second) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> agg_func,
                                   options.GetFieldAggFunc(field));
            if (agg_func) {
                illegal_group.insert(field);
            }
        }
    }

    if (!illegal_group.empty()) {
        std::ostringstream illegal_group_msg;
        illegal_group_msg << "Should not define aggregation function on sequence group: ";
        for (const auto& field : illegal_group) {
            illegal_group_msg << field << " ";
        }
        return Status::Invalid(illegal_group_msg.str());
    }
    return Status::OK();
}

Status SchemaValidation::ValidateSequenceField(const TableSchema& schema,
                                               const CoreOptions& options) {
    std::vector<std::string> sequence_field = options.GetSequenceField();
    if (!sequence_field.empty()) {
        // Create field count map
        std::unordered_map<std::string, int> field_count;
        for (const auto& field : sequence_field) {
            field_count[field]++;
        }

        const auto& field_names = schema.FieldNames();
        for (const auto& field : sequence_field) {
            PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
                std::find(field_names.begin(), field_names.end(), field) != field_names.end(),
                fmt::format("Sequence field: '{}' cannot be found in table schema.", field)));

            PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> agg_func,
                                   options.GetFieldAggFunc(field));
            PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
                agg_func == std::nullopt,
                fmt::format("Should not define aggregation on sequence field: '{}'.", field)));

            PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
                field_count[field] == 1, "Sequence field '" + field + "' is defined repeatedly."));
        }

        // Check for FIRST_ROW merge engine
        if (options.GetMergeEngine() == MergeEngine::FIRST_ROW) {
            return Status::Invalid(
                "Do not support using sequence field on FIRST_ROW merge engine.");
        }

        // Check for cross partition update
        if (schema.CrossPartitionUpdate()) {
            return Status::Invalid(fmt::format(
                "You cannot use sequence.field in cross partition update case (Primary "
                "key constraint '{}'  not including all partition fields '{}').",
                fmt::join(schema.PrimaryKeys(), ", "), fmt::join(schema.PartitionKeys(), ", ")));
        }
    }
    return Status::OK();
}

Status SchemaValidation::ValidateFieldsPrefix(const TableSchema& schema,
                                              const CoreOptions& options) {
    const auto& field_names = schema.FieldNames();
    const auto& options_map = options.ToMap();
    for (const auto& [k, v] : options_map) {
        if (StringUtils::StartsWith(k, Options::FIELDS_PREFIX)) {
            std::vector<std::string> cols = StringUtils::Split(k, ".");
            if (cols.size() < 2) {
                return Status::Invalid("invalid options key " + k);
            }
            std::vector<std::string> fields =
                StringUtils::Split(cols[1], Options::FIELDS_SEPARATOR);
            for (const auto& field : fields) {
                PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
                    Options::DEFAULT_AGG_FUNCTION == field ||
                        std::find(field_names.begin(), field_names.end(), field) !=
                            field_names.end(),
                    "Field " + field + " can not be found in table schema."));
            }
        }
    }
    return Status::OK();
}

Status SchemaValidation::ValidateRowTracking(const TableSchema& table_schema,
                                             const CoreOptions& options) {
    bool row_tracking_enabled = options.RowTrackingEnabled();
    if (row_tracking_enabled) {
        PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
            options.GetBucket() == -1,
            "Cannot define {} for row tracking table, it only support bucket = -1",
            Options::BUCKET));
        PAIMON_RETURN_NOT_OK(
            Preconditions::CheckState(table_schema.PrimaryKeys().empty(),
                                      "Cannot define primary key for row tracking table"));
    }
    if (options.DataEvolutionEnabled()) {
        PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
            row_tracking_enabled, "Data evolution config must enabled with row-tracking.enabled"));
        PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
            !options.DeletionVectorsEnabled(),
            "Data evolution config must disabled with deletion-vectors.enabled"));
    }
    int64_t blob_field_count = 0;
    for (const auto& field : table_schema.Fields()) {
        if (BlobUtils::IsBlobField(field.ArrowField())) {
            blob_field_count += 1;
        }
    }
    if (blob_field_count > 0) {
        PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
            options.DataEvolutionEnabled(),
            "Data evolution config must be enabled for table with BLOB type column."));
        PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
            blob_field_count == 1, "Table with BLOB type column only supports one BLOB column."));
        PAIMON_RETURN_NOT_OK(Preconditions::CheckState(
            table_schema.Fields().size() > 1,
            "Table with BLOB type column must have other normal columns."));
    }
    return Status::OK();
}

}  // namespace paimon
