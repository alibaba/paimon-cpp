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

#include "paimon/common/utils/extend_map_utils.h"

#include <algorithm>

#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "fmt/format.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/compression/block_compressor.h"
#include "paimon/common/compression/block_decompressor.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/options/map_storage_layout.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {
std::string ExtendMapDefine::PhysicalColumnName(int32_t index) {
    return fmt::format("__col_{}", index);
}

// ---- Column detection ----

bool ExtendMapUtils::IsStringKeyMap(const std::shared_ptr<arrow::DataType>& arrow_type) {
    if (arrow_type->id() != arrow::Type::MAP) {
        return false;
    }
    auto map_type = std::static_pointer_cast<arrow::MapType>(arrow_type);
    return map_type->key_type()->id() == arrow::Type::STRING;
}

Result<std::vector<int32_t>> ExtendMapUtils::DetectExtendColumns(
    const std::shared_ptr<arrow::Schema>& schema, const CoreOptions& options) {
    std::vector<int32_t> indices;
    for (int32_t i = 0; i < schema->num_fields(); ++i) {
        const auto& field = schema->field(i);
        PAIMON_ASSIGN_OR_RAISE(MapStorageLayout layout, options.GetMapStorageLayout(field->name()));
        if (layout != MapStorageLayout::EXTEND) {
            continue;
        }
        // Validate: configured column must be MAP<STRING, T>
        if (!IsStringKeyMap(field->type())) {
            return Status::Invalid(
                fmt::format("Column '{}' is configured with map-storage-layout=extend "
                            "but its type is not MAP<STRING, T>",
                            field->name()));
        }
        indices.push_back(i);
    }
    return indices;
}

// ---- Schema conversion ----

std::shared_ptr<arrow::DataType> ExtendMapUtils::BuildPhysicalStructType(
    const std::shared_ptr<arrow::DataType>& value_type, int32_t num_columns, bool value_nullable) {
    arrow::FieldVector struct_fields;
    struct_fields.reserve(num_columns + 2);

    struct_fields.push_back(
        arrow::field(ExtendMapDefine::kFieldMapping, arrow::list(arrow::int32()), false));

    for (int32_t i = 0; i < num_columns; ++i) {
        struct_fields.push_back(
            arrow::field(ExtendMapDefine::PhysicalColumnName(i), value_type, value_nullable));
    }

    struct_fields.push_back(
        arrow::field(ExtendMapDefine::kOverflow, arrow::map(arrow::int32(), value_type), true));

    return arrow::struct_(std::move(struct_fields));
}

Result<std::shared_ptr<arrow::Schema>> ExtendMapUtils::LogicalToPhysicalSchema(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::map<int32_t, int32_t>& column_to_num_columns) {
    arrow::FieldVector physical_fields;
    physical_fields.reserve(logical_schema->num_fields());

    for (int32_t i = 0; i < logical_schema->num_fields(); ++i) {
        const auto& field = logical_schema->field(i);
        auto it = column_to_num_columns.find(i);
        if (it != column_to_num_columns.end()) {
            auto map_type = std::static_pointer_cast<arrow::MapType>(field->type());
            auto value_type = map_type->item_type();
            bool value_nullable = map_type->item_field()->nullable();
            auto physical_type = BuildPhysicalStructType(value_type, it->second, value_nullable);
            physical_fields.push_back(
                arrow::field(field->name(), physical_type, field->nullable()));
        } else {
            physical_fields.push_back(field);
        }
    }

    return arrow::schema(std::move(physical_fields));
}

Result<std::map<int32_t, int32_t>> ExtendMapUtils::BuildColumnToNumColumns(
    const std::vector<int32_t>& extend_column_indices, const std::shared_ptr<arrow::Schema>& schema,
    const CoreOptions& options) {
    std::map<int32_t, int32_t> column_to_num_columns;
    for (int32_t col_index : extend_column_indices) {
        const std::string& field_name = schema->field(col_index)->name();
        PAIMON_ASSIGN_OR_RAISE(int32_t max_columns, options.GetMapExtendMaxColumns(field_name));
        column_to_num_columns[col_index] = max_columns;
    }
    return column_to_num_columns;
}

// ---- Metadata serialization helpers ----

namespace {

std::string JsonEncodeObject(
    const std::function<void(rapidjson::Document&, rapidjson::Document::AllocatorType&)>& builder) {
    rapidjson::Document doc(rapidjson::kObjectType);
    auto& allocator = doc.GetAllocator();
    builder(doc, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

std::string JsonEncodeArray(
    const std::function<void(rapidjson::Document&, rapidjson::Document::AllocatorType&)>& builder) {
    rapidjson::Document doc(rapidjson::kArrayType);
    auto& allocator = doc.GetAllocator();
    builder(doc, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

Result<std::string> CompressString(const std::string& input, const std::string& compression) {
    CompressOptions compress_opts{compression, /*zstd_level=*/1};
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockCompressionFactory> factory,
                           BlockCompressionFactory::Create(compress_opts));
    std::shared_ptr<BlockCompressor> compressor = factory->GetCompressor();
    if (!compressor) {
        return input;
    }

    int32_t src_size = static_cast<int32_t>(input.size());
    int32_t max_compressed = compressor->GetMaxCompressedSize(src_size);
    std::string output(max_compressed, '\0');

    PAIMON_ASSIGN_OR_RAISE(
        int32_t actual_size,
        compressor->Compress(input.data(), src_size, output.data(), max_compressed));

    output.resize(actual_size);
    return output;
}

Result<std::string> DecompressString(const std::string& input, int32_t original_len,
                                     const std::string& compression) {
    CompressOptions compress_opts{compression, /*zstd_level=*/1};
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockCompressionFactory> factory,
                           BlockCompressionFactory::Create(compress_opts));
    std::shared_ptr<BlockDecompressor> decompressor = factory->GetDecompressor();
    if (!decompressor) {
        return input;
    }
    std::string output(original_len, '\0');
    PAIMON_ASSIGN_OR_RAISE(
        int32_t decompressed_len,
        decompressor->Decompress(input.data(), static_cast<int32_t>(input.size()), output.data(),
                                 original_len));
    output.resize(decompressed_len);
    return output;
}

Result<std::string> GetRequiredValue(const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
                                     const char* key) {
    int32_t index = metadata->FindKey(key);
    if (index < 0) {
        return Status::Invalid(fmt::format("missing extend metadata key: {}", key));
    }
    return metadata->value(index);
}

Result<int32_t> GetRequiredInt32(const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
                                 const char* key) {
    PAIMON_ASSIGN_OR_RAISE(std::string value, GetRequiredValue(metadata, key));
    std::optional<int32_t> parsed = StringUtils::StringToValue<int32_t>(value);
    if (!parsed.has_value()) {
        return Status::Invalid(fmt::format("malformed extend metadata value for key: {}", key));
    }
    return parsed.value();
}

std::string SerializeFieldDict(const ExtendMapFileMetadata& file_metadata) {
    return JsonEncodeObject([&](auto& doc, auto& alloc) {
        for (const auto& [name, id] : file_metadata.name_to_id) {
            doc.AddMember(rapidjson::Value(name.c_str(), alloc), rapidjson::Value(id), alloc);
        }
    });
}

std::string SerializeFieldColumns(const ExtendMapFileMetadata& file_metadata) {
    return JsonEncodeObject([&](auto& doc, auto& alloc) {
        for (const auto& [field_id, col_vec] : file_metadata.field_to_columns) {
            rapidjson::Value array(rapidjson::kArrayType);
            std::vector<int32_t> sorted_cols(col_vec.begin(), col_vec.end());
            std::sort(sorted_cols.begin(), sorted_cols.end());
            for (int32_t col : sorted_cols) {
                array.PushBack(col, alloc);
            }
            std::string key = std::to_string(field_id);
            doc.AddMember(rapidjson::Value(key.c_str(), alloc), array, alloc);
        }
    });
}

std::string SerializeOverflowSet(const ExtendMapFileMetadata& file_metadata) {
    return JsonEncodeArray([&](auto& doc, auto& alloc) {
        std::vector<int32_t> sorted(file_metadata.overflow_field_set.begin(),
                                    file_metadata.overflow_field_set.end());
        std::sort(sorted.begin(), sorted.end());
        for (int32_t field_id : sorted) {
            doc.PushBack(field_id, alloc);
        }
    });
}

Result<std::map<std::string, int32_t>> DeserializeFieldDict(const std::string& json_str) {
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    if (doc.HasParseError() || !doc.IsObject()) {
        return Status::Invalid("malformed extend field_dict metadata");
    }
    std::map<std::string, int32_t> name_to_id;
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        name_to_id[it->name.GetString()] = it->value.GetInt();
    }
    return name_to_id;
}

Result<std::map<int32_t, std::vector<int32_t>>> DeserializeFieldColumns(
    const std::string& json_str) {
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    if (doc.HasParseError() || !doc.IsObject()) {
        return Status::Invalid("malformed extend field_columns metadata");
    }
    std::map<int32_t, std::vector<int32_t>> field_to_columns;
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        std::optional<int32_t> field_id = StringUtils::StringToValue<int32_t>(it->name.GetString());
        if (!field_id.has_value()) {
            return Status::Invalid("malformed extend field_columns: invalid field_id key");
        }
        const auto& array = it->value;
        if (!array.IsArray()) {
            return Status::Invalid("malformed extend field_columns: value is not array");
        }
        std::vector<int32_t> cols;
        cols.reserve(array.Size());
        for (rapidjson::SizeType i = 0; i < array.Size(); ++i) {
            cols.push_back(array[i].GetInt());
        }
        field_to_columns[field_id.value()] = std::move(cols);
    }
    return field_to_columns;
}

Result<std::set<int32_t>> DeserializeOverflowSet(const std::string& json_str) {
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    if (doc.HasParseError() || !doc.IsArray()) {
        return Status::Invalid("malformed extend overflow_set metadata");
    }
    std::set<int32_t> overflow_set;
    for (rapidjson::SizeType i = 0; i < doc.Size(); ++i) {
        overflow_set.insert(doc[i].GetInt());
    }
    return overflow_set;
}

}  // namespace

Status ExtendMapUtils::SerializeMetadata(const ExtendMapFileMetadata& file_metadata,
                                         const std::string& compression,
                                         arrow::KeyValueMetadata* metadata) {
    metadata->Append(ExtendMapDefine::kVersion, std::to_string(ExtendMapDefine::kCurrentVersion));
    metadata->Append(ExtendMapDefine::kStorageLayout, ExtendMapDefine::kStorageLayoutExtend);

    std::string field_dict_json = SerializeFieldDict(file_metadata);
    metadata->Append(ExtendMapDefine::kFieldDictOriginalSize,
                     std::to_string(field_dict_json.size()));
    PAIMON_ASSIGN_OR_RAISE(std::string compressed_dict,
                           CompressString(field_dict_json, compression));
    metadata->Append(ExtendMapDefine::kFieldDict, std::move(compressed_dict));

    metadata->Append(ExtendMapDefine::kFieldColumns, SerializeFieldColumns(file_metadata));
    metadata->Append(ExtendMapDefine::kOverflowSet, SerializeOverflowSet(file_metadata));
    metadata->Append(ExtendMapDefine::kNumColumns, std::to_string(file_metadata.num_columns));
    metadata->Append(ExtendMapDefine::kMaxRowWidth, std::to_string(file_metadata.max_row_width));

    return Status::OK();
}

Result<ExtendMapFileMetadata> ExtendMapUtils::DeserializeMetadata(
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata, const std::string& compression) {
    if (!metadata) {
        return Status::Invalid("metadata is null");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t version, GetRequiredInt32(metadata, ExtendMapDefine::kVersion));
    if (version != ExtendMapDefine::kCurrentVersion) {
        return Status::Invalid(
            fmt::format("unsupported extend-map metadata version: {}, expected: {}", version,
                        ExtendMapDefine::kCurrentVersion));
    }

    ExtendMapFileMetadata result;

    // field_dict (compressed)
    PAIMON_ASSIGN_OR_RAISE(int32_t original_len,
                           GetRequiredInt32(metadata, ExtendMapDefine::kFieldDictOriginalSize));
    PAIMON_ASSIGN_OR_RAISE(std::string compressed_dict,
                           GetRequiredValue(metadata, ExtendMapDefine::kFieldDict));
    PAIMON_ASSIGN_OR_RAISE(std::string field_dict_json,
                           DecompressString(compressed_dict, original_len, compression));
    PAIMON_ASSIGN_OR_RAISE(result.name_to_id, DeserializeFieldDict(field_dict_json));

    // field_columns
    PAIMON_ASSIGN_OR_RAISE(std::string field_columns_json,
                           GetRequiredValue(metadata, ExtendMapDefine::kFieldColumns));
    PAIMON_ASSIGN_OR_RAISE(result.field_to_columns, DeserializeFieldColumns(field_columns_json));

    // overflow_set
    PAIMON_ASSIGN_OR_RAISE(std::string overflow_json,
                           GetRequiredValue(metadata, ExtendMapDefine::kOverflowSet));
    PAIMON_ASSIGN_OR_RAISE(result.overflow_field_set, DeserializeOverflowSet(overflow_json));

    // num_columns & max_row_width
    PAIMON_ASSIGN_OR_RAISE(result.num_columns,
                           GetRequiredInt32(metadata, ExtendMapDefine::kNumColumns));
    PAIMON_ASSIGN_OR_RAISE(result.max_row_width,
                           GetRequiredInt32(metadata, ExtendMapDefine::kMaxRowWidth));

    return result;
}

bool ExtendMapUtils::HasExtendMetadata(const std::shared_ptr<arrow::KeyValueMetadata>& metadata) {
    if (!metadata) {
        return false;
    }
    auto index = metadata->FindKey(ExtendMapDefine::kStorageLayout);
    if (index < 0) {
        return false;
    }
    return metadata->value(index) == ExtendMapDefine::kStorageLayoutExtend;
}

}  // namespace paimon
