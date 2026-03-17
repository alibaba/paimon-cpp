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

#include "paimon/core/io/file_index_evaluator.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <set>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;
enum class FieldType;

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::Evaluate(
    const std::shared_ptr<arrow::Schema>& data_schema, const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::shared_ptr<DataFileMeta>& file_meta, const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& pool) {
    return Evaluate(/*only_use_embedding_index=*/false, data_schema, predicate,
                    data_file_path_factory, file_meta, file_system, pool);
}

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::Evaluate(
    const std::shared_ptr<arrow::Schema>& data_schema, const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<DataFileMeta>& file_meta, const std::shared_ptr<MemoryPool>& pool) {
    return Evaluate(/*only_use_embedding_index=*/true, data_schema, predicate,
                    /*data_file_path_factory=*/nullptr, file_meta, /*file_system=*/nullptr, pool);
}

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::Evaluate(
    bool only_use_embedding_index, const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::shared_ptr<DataFileMeta>& file_meta, const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& pool) {
    if (predicate == nullptr) {
        return FileIndexResult::Remain();
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                           ExtractIndexInputStream(only_use_embedding_index, data_file_path_factory,
                                                   file_meta, file_system));
    if (!input_stream) {
        // no index files, just return REMAIN
        return FileIndexResult::Remain();
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexFormat::Reader> format_reader,
                           FileIndexFormat::CreateReader(input_stream, pool));
    std::set<std::string> required_field_names;
    PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(predicate, &required_field_names));
    std::map<std::string, std::vector<std::shared_ptr<FileIndexReader>>>
        field_name_to_index_readers;
    for (const auto& field_name : required_field_names) {
        auto data_field = data_schema->GetFieldByName(field_name);
        if (data_field == nullptr) {
            return Status::Invalid(fmt::format("field {} not exist in data schema {}", field_name,
                                               data_schema->ToString()));
        }
        ArrowSchema c_arrow_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportSchema(*arrow::schema({data_field}), &c_arrow_schema));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<FileIndexReader>> index_readers,
                               format_reader->ReadColumnIndex(field_name, &c_arrow_schema));
        field_name_to_index_readers.emplace(field_name, std::move(index_readers));
    }
    return Evaluate(predicate, field_name_to_index_readers);
}

Result<std::shared_ptr<InputStream>> FileIndexEvaluator::ExtractIndexInputStream(
    bool only_use_embedding_index,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::shared_ptr<DataFileMeta>& file_meta,
    const std::shared_ptr<FileSystem>& file_system) {
    const auto& embedded_index = file_meta->embedded_index;
    if (embedded_index != nullptr) {
        // inline index
        return std::make_shared<ByteArrayInputStream>(embedded_index->data(),
                                                      embedded_index->size());
    }
    if (only_use_embedding_index) {
        return std::shared_ptr<InputStream>();
    }
    if (!data_file_path_factory || !file_system) {
        return Status::Invalid(
            "read process for FileIndexEvaluator must have data_file_path_factory and file_system");
    }
    // get input stream from file
    std::vector<std::string> index_files;
    for (const auto& extra_file : file_meta->extra_files) {
        if (extra_file) {
            if (StringUtils::EndsWith(extra_file.value(), DataFilePathFactory::INDEX_PATH_SUFFIX)) {
                index_files.emplace_back(extra_file.value());
            }
        }
    }
    if (!index_files.empty()) {
        if (index_files.size() > 1) {
            return Status::Invalid(
                fmt::format("Found more than one index file for one data file: {}",
                            fmt::join(index_files, ", ")));
        }
        std::string file_index_path =
            data_file_path_factory->ToAlignedPath(index_files[0], file_meta);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                               file_system->Open(file_index_path));
        return input_stream;
    }
    // no index
    return std::shared_ptr<InputStream>();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::Evaluate(
    const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, std::vector<std::shared_ptr<FileIndexReader>>>&
        field_name_to_index_readers) {
    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return EvaluateCompoundPredicate(compound_predicate, field_name_to_index_readers);
    } else if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        std::shared_ptr<FileIndexResult> compound_result = FileIndexResult::Remain();
        // TODO(xinyu.lxy): erase predicate, rm predicate which hits bitmap index
        auto iter = field_name_to_index_readers.find(leaf_predicate->FieldName());
        if (iter == field_name_to_index_readers.end()) {
            assert(false);
            return Status::Invalid(fmt::format(
                "field name {} in leaf literal does not exist in field_name_to_index_readers",
                leaf_predicate->FieldName()));
        }
        for (const auto& index_reader : iter->second) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> sub_result,
                                   PredicateUtils::VisitPredicate<std::shared_ptr<FileIndexResult>>(
                                       leaf_predicate, index_reader));
            PAIMON_ASSIGN_OR_RAISE(compound_result, compound_result->And(sub_result));
            PAIMON_ASSIGN_OR_RAISE(bool is_remain, compound_result->IsRemain());
            if (!is_remain) {
                return compound_result;
            }
        }
        assert(compound_result);
        // if index readers is empty (the index type is not registered), return
        // FileIndexResult::Remain()
        return compound_result;
    }
    return Status::Invalid(fmt::format(
        "cannot cast predicate {} to CompoundPredicate or LeafPredicate", predicate->ToString()));
}

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::EvaluateCompoundPredicate(
    const std::shared_ptr<CompoundPredicate>& compound_predicate,
    const std::map<std::string, std::vector<std::shared_ptr<FileIndexReader>>>&
        field_name_to_index_readers) {
    if (compound_predicate->GetFunction().GetType() == Function::Type::OR) {
        std::shared_ptr<FileIndexResult> compound_result;
        for (const auto& child : compound_predicate->Children()) {
            if (compound_result == nullptr) {
                PAIMON_ASSIGN_OR_RAISE(compound_result,
                                       Evaluate(child, field_name_to_index_readers));
            } else {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> child_result,
                                       Evaluate(child, field_name_to_index_readers));
                PAIMON_ASSIGN_OR_RAISE(compound_result, compound_result->Or(child_result));
            }
        }
        assert(compound_result);
        return compound_result;
    } else if (compound_predicate->GetFunction().GetType() == Function::Type::AND) {
        std::shared_ptr<FileIndexResult> compound_result;
        for (const auto& child : compound_predicate->Children()) {
            if (compound_result == nullptr) {
                PAIMON_ASSIGN_OR_RAISE(compound_result,
                                       Evaluate(child, field_name_to_index_readers));
            } else {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> child_result,
                                       Evaluate(child, field_name_to_index_readers));
                PAIMON_ASSIGN_OR_RAISE(compound_result, compound_result->And(child_result));
            }
            // if not remain, no need to test anymore
            PAIMON_ASSIGN_OR_RAISE(bool is_remain, compound_result->IsRemain());
            if (!is_remain) {
                return compound_result;
            }
        }
        assert(compound_result);
        return compound_result;
    }
    return Status::Invalid("CompoundPredicate only support And/Or function");
}

}  // namespace paimon
