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
#include "paimon/global_index/lucene/lucene_global_index.h"

#include <filesystem>

#include "arrow/c/bridge.h"
#include "lucene++/FileUtils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/global_index/lucene/lucene_global_index_reader.h"
#include "paimon/global_index/lucene/lucene_global_index_writer.h"
#include "paimon/global_index/lucene/lucene_utils.h"
namespace paimon::lucene {
#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

LuceneGlobalIndex::LuceneGlobalIndex(const std::map<std::string, std::string>& options)
    : options_(OptionsUtils::FetchOptionsWithPrefix(kOptionKeyPrefix, options)) {}

Result<std::shared_ptr<GlobalIndexWriter>> LuceneGlobalIndex::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    // check data type
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    CHECK_NOT_NULL(struct_type, "arrow schema must be struct type when create LuceneIndexWriter");
    auto index_field = struct_type->GetFieldByName(field_name);
    CHECK_NOT_NULL(index_field,
                   fmt::format("field {} not exist in arrow schema when create LuceneIndexWriter",
                               field_name));
    if (index_field->type()->id() != arrow::Type::type::STRING) {
        return Status::Invalid("field type must be string");
    }
    return LuceneGlobalIndexWriter::Create(field_name, arrow_type, file_writer, options_, pool);
}

Result<std::shared_ptr<GlobalIndexReader>> LuceneGlobalIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (files.size() != 1) {
        return Status::Invalid("lucene index only has one index file per shard");
    }
    const auto& io_meta = files[0];
    // check data type
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid("LuceneGlobalIndex now only support one field");
    }
    auto index_field = arrow_schema->field(0);
    if (index_field->type()->id() != arrow::Type::type::STRING) {
        return Status::Invalid("field type must be string");
    }
    return LuceneGlobalIndexReader::Create(index_field->name(), io_meta, file_reader, options_,
                                           pool);
}

}  // namespace paimon::lucene
