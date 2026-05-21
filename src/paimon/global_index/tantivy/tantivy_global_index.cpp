/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

#include "paimon/global_index/tantivy/tantivy_global_index.h"

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/global_index/tantivy/tantivy_global_index_reader.h"
#include "paimon/global_index/tantivy/tantivy_global_index_writer.h"

namespace paimon::tantivy {

#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

TantivyGlobalIndex::TantivyGlobalIndex(const std::map<std::string, std::string>& options)
    : options_(OptionsUtils::FetchOptionsWithPrefix(kOptionKeyPrefix, options)) {}

Result<std::shared_ptr<GlobalIndexWriter>> TantivyGlobalIndex::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    CHECK_NOT_NULL(struct_type,
                   "arrow schema must be struct type when create TantivyGlobalIndexWriter");
    auto index_field = struct_type->GetFieldByName(field_name);
    CHECK_NOT_NULL(
        index_field,
        fmt::format("field {} not exist in arrow schema when create TantivyGlobalIndexWriter",
                    field_name));
    if (index_field->type()->id() != arrow::Type::type::STRING) {
        return Status::Invalid("field type must be string");
    }
    return TantivyGlobalIndexWriter::Create(field_name, arrow_type, file_writer, options_, pool);
}

Result<std::shared_ptr<GlobalIndexReader>> TantivyGlobalIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (files.size() != 1) {
        return Status::Invalid("tantivy index only has one index file per shard, now num: {}" , files.size());
    }
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid("TantivyGlobalIndex now only support one field");
    }
    auto index_field = arrow_schema->field(0);
    if (index_field->type()->id() != arrow::Type::type::STRING) {
        return Status::Invalid("field type must be string");
    }
    return TantivyGlobalIndexReader::Create(index_field->name(), files[0], file_reader, options_,
                                            pool);
}

}  // namespace paimon::tantivy
