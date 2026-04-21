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

#include "paimon/core/operation/merge_file_split_read.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <map>
#include <optional>
#include <set>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/apply_deletion_vector_batch_reader.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/io/async_key_value_projection_reader.h"
#include "paimon/core/io/concat_key_value_record_reader.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/key_value_data_file_record_reader.h"
#include "paimon/core/io/key_value_projection_reader.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/mergetree/compact/lookup_merge_function.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/mergetree/compact/partial_update_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_loser_tree.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_min_heap.h"
#include "paimon/core/mergetree/drop_delete_reader.h"
#include "paimon/core/mergetree/sorted_run.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/options/sort_engine.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/table/source/data_split.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class BinaryRow;
class DataFilePathFactory;
class Executor;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

Result<std::unique_ptr<MergeFileSplitRead>> MergeFileSplitRead::Create(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor) {
    const auto& core_options = context->GetCoreOptions();
    const auto& table_schema = context->GetTableSchema();
    assert(table_schema);
    // value_schema is the schema of member value in KeyValue Object
    std::shared_ptr<arrow::Schema> value_schema;
    // read_schema is the read schema for format file reader (e.g., includes _SEQUENCE_NUMBER)
    std::shared_ptr<arrow::Schema> read_schema;
    // comparator of member key in KeyValue object
    std::shared_ptr<FieldsComparator> key_comparator;
    // comparator of user-defined sequence fields in member value of KeyValue object
    std::shared_ptr<FieldsComparator> user_defined_seq_comparator;

    PAIMON_RETURN_NOT_OK(GenerateKeyValueReadSchema(
        *table_schema, core_options, context->GetReadSchema(), &value_schema, &read_schema,
        &key_comparator, &user_defined_seq_comparator));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> predicate_for_keys,
                           GenerateKeyPredicates(context->GetPredicate(), *table_schema));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> key_schema,
                           table_schema->TrimmedPrimaryKeySchema());

    // projection is the mapping from value_schema in KeyValue object to raw_read_schema
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<int32_t> projection,
        ArrowUtils::CreateProjection(value_schema, context->GetReadSchema()->fields()));

    return std::unique_ptr<MergeFileSplitRead>(new MergeFileSplitRead(
        path_factory, context,
        std::make_unique<SchemaManager>(core_options.GetFileSystem(), context->GetPath(),
                                        context->GetCoreOptions().GetBranch()),
        key_schema, value_schema, read_schema, projection, key_comparator,
        user_defined_seq_comparator, predicate_for_keys, memory_pool, executor));
}

Result<std::unique_ptr<BatchReader>> MergeFileSplitRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split) {
        return Status::Invalid("cannot cast split to data_split in MergeFileSplitRead");
    }
    if (!data_split->BeforeFiles().empty()) {
        return Status::Invalid("this read cannot accept split with before files.");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataFilePathFactory> data_file_path_factory,
        path_factory_->CreateDataFilePathFactory(data_split->Partition(), data_split->Bucket()));
    std::unique_ptr<BatchReader> batch_reader;
    if (data_split->IsStreaming() || data_split->Bucket() == BucketModeDefine::POSTPONE_BUCKET) {
        PAIMON_ASSIGN_OR_RAISE(
            batch_reader,
            CreateNoMergeReader(data_split, /*only_filter_key=*/data_split->IsStreaming(),
                                data_file_path_factory));
    } else {
        PAIMON_ASSIGN_OR_RAISE(batch_reader, CreateMergeReader(data_split, data_file_path_factory));
    }
    return std::make_unique<CompleteRowKindBatchReader>(std::move(batch_reader), pool_);
}

void MergeFileSplitRead::SetMergeFunctionWrapper(
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper) {
    merge_function_wrapper_ = merge_function_wrapper;
}

Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>>
MergeFileSplitRead::GetMergeFunctionWrapper() {
    if (!merge_function_wrapper_) {
        // In deletion vector mode, streaming data split or postpone bucket mode, we don't need
        // to use merge function. Even if the merge function in CoreOptions is not supported, it
        // should not affect data reading. So we create merge_function_wrapper_ lazily, to avoid
        // raise errors when creating MergeFileSplitRead at the beginning.
        PAIMON_ASSIGN_OR_RAISE(
            merge_function_wrapper_,
            CreateMergeFunctionWrapper(options_, context_->GetTableSchema(), value_schema_));
    }
    return merge_function_wrapper_;
}

Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>>
MergeFileSplitRead::CreateMergeFunctionWrapper(const CoreOptions& core_options,
                                               const std::shared_ptr<TableSchema>& table_schema,
                                               const std::shared_ptr<arrow::Schema>& value_schema) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeFunction> merge_function,
                           PrimaryKeyTableUtils::CreateMergeFunction(
                               value_schema, table_schema->PrimaryKeys(), core_options));
    if (core_options.NeedLookup() && core_options.GetMergeEngine() != MergeEngine::FIRST_ROW) {
        // don't wrap first row, it is already OK
        merge_function = std::make_unique<LookupMergeFunction>(std::move(merge_function));
    }
    return std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
}

Result<std::unique_ptr<FileBatchReader>> MergeFileSplitRead::ApplyIndexAndDvReaderIfNeeded(
    std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    DeletionVector::Factory dv_factory, const std::optional<std::vector<Range>>& ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    // merge read does not use index
    std::shared_ptr<DeletionVector> deletion_vector;
    if (dv_factory) {
        PAIMON_ASSIGN_OR_RAISE(deletion_vector, dv_factory(file->file_name));
    }

    const RoaringBitmap32* deletion = nullptr;
    if (auto* bitmap_dv = dynamic_cast<BitmapDeletionVector*>(deletion_vector.get())) {
        deletion = bitmap_dv->GetBitmap();
    }

    std::optional<RoaringBitmap32> actual_selection;
    if (deletion) {
        actual_selection = *deletion;
        PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, file_reader->GetNumberOfRows());
        actual_selection.value().Flip(0, num_rows);
    }

    ::ArrowSchema c_read_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_read_schema));

    PAIMON_RETURN_NOT_OK(file_reader->SetReadSchema(&c_read_schema, predicate, actual_selection));

    if (!file_reader->SupportPreciseBitmapSelection() && actual_selection) {
        return std::make_unique<ApplyDeletionVectorBatchReader>(std::move(file_reader),
                                                                deletion_vector);
    }
    if (deletion_vector && !deletion && !deletion_vector->IsEmpty()) {
        // TODO(xinyu.lxy): if deletion vector is bitmap64, use ApplyBitmapIndexBatchReader to
        // filter result
        return Status::NotImplemented("Only support BitmapDeletionVector");
    }
    return std::move(file_reader);
}

Result<std::unique_ptr<BatchReader>> MergeFileSplitRead::CreateMergeReader(
    const std::shared_ptr<DataSplitImpl>& data_split,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) {
    auto dv_factory = DeletionVector::CreateFactory(options_.GetFileSystem(),
                                                    CreateDeletionFileMap(*data_split), pool_);

    std::vector<std::vector<SortedRun>> sections =
        IntervalPartition(data_split->DataFiles(), key_comparator_).Partition();
    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    batch_readers.reserve(sections.size());
    // no overlap through multiple sections
    for (const auto& section : sections) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> projection_reader,
                               CreateReaderForSection(section, data_split->Partition(), dv_factory,
                                                      data_file_path_factory));
        batch_readers.push_back(std::move(projection_reader));
    }
    auto concat_batch_reader = std::make_unique<ConcatBatchReader>(std::move(batch_readers), pool_);
    return AbstractSplitRead::ApplyPredicateFilterIfNeeded(std::move(concat_batch_reader),
                                                           context_->GetPredicate());
}

Result<std::unique_ptr<BatchReader>> MergeFileSplitRead::CreateNoMergeReader(
    const std::shared_ptr<DataSplitImpl>& data_split, bool only_filter_key,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    auto dv_factory = DeletionVector::CreateFactory(options_.GetFileSystem(),
                                                    CreateDeletionFileMap(*data_split), pool_);

    // create read schema without extra fields (e.g., completed key, sequence fields)
    auto row_kind_field = DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind());

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                      raw_read_schema_->AddField(0, row_kind_field));
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<FileBatchReader>> raw_file_readers,
        CreateRawFileReaders(data_split->Partition(), data_split->DataFiles(), read_schema,
                             only_filter_key ? predicate_for_keys_ : context_->GetPredicate(),
                             dv_factory, /*row_ranges=*/{}, data_file_path_factory));

    auto raw_readers =
        ObjectUtils::MoveVector<std::unique_ptr<BatchReader>>(std::move(raw_file_readers));
    auto concat_batch_reader = std::make_unique<ConcatBatchReader>(std::move(raw_readers), pool_);
    return AbstractSplitRead::ApplyPredicateFilterIfNeeded(std::move(concat_batch_reader),
                                                           context_->GetPredicate());
}

MergeFileSplitRead::MergeFileSplitRead(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    std::unique_ptr<SchemaManager>&& schema_manager,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::vector<int32_t>& projection,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<Predicate>& predicate_for_keys,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor)
    : AbstractSplitRead(path_factory, context, std::move(schema_manager), memory_pool, executor),
      key_schema_(key_schema),
      value_schema_(value_schema),
      read_schema_(read_schema),
      projection_(projection),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      predicate_for_keys_(predicate_for_keys) {}

Status MergeFileSplitRead::GenerateKeyValueReadSchema(
    const TableSchema& table_schema, const CoreOptions& options,
    const std::shared_ptr<arrow::Schema>& raw_read_schema,
    std::shared_ptr<arrow::Schema>* value_schema, std::shared_ptr<arrow::Schema>* read_schema,
    std::shared_ptr<FieldsComparator>* key_comparator,
    std::shared_ptr<FieldsComparator>* sequence_fields_comparator) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_key_fields,
                           table_schema.TrimmedPrimaryKeyFields());
    PAIMON_ASSIGN_OR_RAISE(*key_comparator, FieldsComparator::Create(trimmed_key_fields,
                                                                     /*is_ascending_order=*/true));
    const auto& table_fields = table_schema.Fields();
    auto table_fields_schema = DataField::ConvertDataFieldsToArrowSchema(table_fields);
    if (table_fields_schema->Equals(raw_read_schema)) {
        // Short-circuit: if raw_read_schema is the same as the table schema,
        // use the table schema field order directly (for compact process).
        *value_schema = table_fields_schema;
        // sequence_fields_comparator
        PAIMON_ASSIGN_OR_RAISE(
            *sequence_fields_comparator,
            PrimaryKeyTableUtils::CreateSequenceFieldsComparator(table_fields, options));
        *read_schema = SpecialFields::CompleteSequenceAndValueKindField(*value_schema);
        return Status::OK();
    }

    // 1. add user raw read schema to need_fields
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> need_fields,
                           DataField::ConvertArrowSchemaToDataFields(raw_read_schema));
    // 2. add user defined sequence field to need_fields
    PAIMON_RETURN_NOT_OK(CompleteSequenceField(table_schema, options, &need_fields));
    if (options.GetMergeEngine() == MergeEngine::PARTIAL_UPDATE) {
        // add sequence group fields for partial update
        std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
        std::set<std::string> seq_group_key_set;
        PAIMON_RETURN_NOT_OK(PartialUpdateMergeFunction::ParseSequenceGroupFields(
            options, &value_field_to_seq_group_field, &seq_group_key_set));
        PAIMON_RETURN_NOT_OK(PartialUpdateMergeFunction::CompleteSequenceGroupFields(
            table_schema, value_field_to_seq_group_field, &need_fields));
    }
    // 3. split need_fields to key and non-key fields
    std::vector<DataField> key_fields;
    std::vector<DataField> non_key_fields;
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_key_names,
                           table_schema.TrimmedPrimaryKeys());
    PAIMON_RETURN_NOT_OK(
        SplitKeyAndNonKeyField(trimmed_key_names, need_fields, &key_fields, &non_key_fields));

    // 4. construct value fields: key fields are put before non-key fields
    std::vector<DataField> value_fields;
    value_fields.insert(value_fields.end(), key_fields.begin(), key_fields.end());
    value_fields.insert(value_fields.end(), non_key_fields.begin(), non_key_fields.end());
    *value_schema = DataField::ConvertDataFieldsToArrowSchema(value_fields);
    // 5. create sequence field comparator
    PAIMON_ASSIGN_OR_RAISE(
        *sequence_fields_comparator,
        PrimaryKeyTableUtils::CreateSequenceFieldsComparator(value_fields, options));
    // 6. construct actual read fields: special + key + non-key value
    std::vector<DataField> read_fields = {SpecialFields::SequenceNumber(),
                                          SpecialFields::ValueKind()};
    read_fields.insert(read_fields.end(), trimmed_key_fields.begin(), trimmed_key_fields.end());
    read_fields.insert(read_fields.end(), non_key_fields.begin(), non_key_fields.end());
    *read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    return Status::OK();
}

Status MergeFileSplitRead::SplitKeyAndNonKeyField(
    const std::vector<std::string>& trimmed_key_fields, const std::vector<DataField>& read_fields,
    std::vector<DataField>* key_fields, std::vector<DataField>* non_key_fields) {
    for (const auto& field : read_fields) {
        auto iter = std::find(trimmed_key_fields.begin(), trimmed_key_fields.end(), field.Name());
        if (iter == trimmed_key_fields.end()) {
            non_key_fields->push_back(field);
        } else {
            key_fields->push_back(field);
        }
    }
    return Status::OK();
}

Status MergeFileSplitRead::CompleteSequenceField(const TableSchema& table_schema,
                                                 const CoreOptions& options,
                                                 std::vector<DataField>* non_key_fields) {
    auto sequence_field_names = options.GetSequenceField();
    if (sequence_field_names.empty()) {
        return Status::OK();
    }

    std::set<std::string> non_key_field_names;
    for (const auto& field : *non_key_fields) {
        non_key_field_names.insert(field.Name());
    }

    for (const auto& seq_field_name : sequence_field_names) {
        auto iter = non_key_field_names.find(seq_field_name);
        if (iter == non_key_field_names.end()) {
            // force add sequence fields
            PAIMON_ASSIGN_OR_RAISE(DataField seq_field, table_schema.GetField(seq_field_name));
            non_key_fields->push_back(seq_field);
        }
    }
    return Status::OK();
}

Result<std::shared_ptr<Predicate>> MergeFileSplitRead::GenerateKeyPredicates(
    const std::shared_ptr<Predicate>& predicate, const TableSchema& table_schema) {
    // extract predicates only contain trimmed key fields
    if (!predicate) {
        return std::shared_ptr<Predicate>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_key_fields,
                           table_schema.TrimmedPrimaryKeys());
    std::set<std::string> non_primary_keys;
    for (const auto& field_name : table_schema.FieldNames()) {
        auto iter = std::find(trimmed_key_fields.begin(), trimmed_key_fields.end(), field_name);
        if (iter == trimmed_key_fields.end()) {
            non_primary_keys.insert(field_name);
        }
    }
    return PredicateUtils::ExcludePredicateWithFields(predicate, non_primary_keys);
}

Result<std::unique_ptr<BatchReader>> MergeFileSplitRead::CreateReaderForSection(
    const std::vector<SortedRun>& section, const BinaryRow& partition,
    DeletionVector::Factory dv_factory,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) {
    // with overlap in one section
    std::shared_ptr<Predicate> predicate;
    if (section.size() > 1) {
        predicate = predicate_for_keys_;
    } else {
        predicate = context_->GetPredicate();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<SortMergeReader> sort_merge_reader,
        CreateSortMergeReaderForSection(section, partition, dv_factory, predicate,
                                        data_file_path_factory, /*drop_delete=*/true));
    // KeyValueProjectionReader converts KeyValue objects to arrow array according to projection
    if (!context_->EnableMultiThreadRowToBatch()) {
        return KeyValueProjectionReader::Create(std::move(sort_merge_reader), raw_read_schema_,
                                                projection_, options_.GetReadBatchSize(), pool_);
    }
    int32_t thread_number = context_->GetRowToBatchThreadNumber();
    assert(thread_number > 0);
    return std::make_unique<AsyncKeyValueProjectionReader>(
        std::move(sort_merge_reader), raw_read_schema_, projection_, options_.GetReadBatchSize(),
        thread_number, pool_);
}

Result<std::unique_ptr<SortMergeReader>> MergeFileSplitRead::CreateSortMergeReaderForSection(
    const std::vector<SortedRun>& section, const BinaryRow& partition,
    DeletionVector::Factory dv_factory, const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory, bool drop_delete) {
    // with overlap in one section
    std::vector<std::unique_ptr<KeyValueRecordReader>> record_readers;
    record_readers.reserve(section.size());
    for (const auto& run : section) {
        // no overlap in a run
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<KeyValueRecordReader> run_reader,
            CreateReaderForRun(partition, run, dv_factory, predicate, data_file_path_factory));
        record_readers.emplace_back(std::move(run_reader));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader> sort_merge_reader,
                           CreateSortMergeReader(std::move(record_readers)));
    if (drop_delete) {
        sort_merge_reader = std::make_unique<DropDeleteReader>(std::move(sort_merge_reader));
    }
    return sort_merge_reader;
}

Result<std::unique_ptr<KeyValueRecordReader>> MergeFileSplitRead::CreateReaderForRun(
    const BinaryRow& partition, const SortedRun& sorted_run, DeletionVector::Factory dv_factory,
    const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    // no overlap in a run
    const auto& data_files = sorted_run.Files();
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<FileBatchReader>> raw_file_readers,
        CreateRawFileReaders(partition, data_files, read_schema_, predicate, dv_factory,
                             /*row_ranges=*/{}, data_file_path_factory));

    assert(data_files.size() == raw_file_readers.size());
    // KeyValueDataFileRecordReader converts arrow array from format reader to KeyValue objects
    std::vector<std::unique_ptr<KeyValueRecordReader>> file_record_readers;
    file_record_readers.reserve(data_files.size());
    for (size_t i = 0; i < data_files.size(); i++) {
        file_record_readers.push_back(std::make_unique<KeyValueDataFileRecordReader>(
            std::move(raw_file_readers[i]), key_schema_, value_schema_, data_files[i]->level,
            pool_));
    }
    return std::make_unique<ConcatKeyValueRecordReader>(std::move(file_record_readers));
}

Result<std::unique_ptr<SortMergeReader>> MergeFileSplitRead::CreateSortMergeReader(
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& record_readers) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper,
                           GetMergeFunctionWrapper());
    auto sort_engine = options_.GetSortEngine();
    if (sort_engine == SortEngine::MIN_HEAP) {
        return std::make_unique<SortMergeReaderWithMinHeap>(
            std::move(record_readers), key_comparator_, user_defined_seq_comparator_,
            merge_function_wrapper);
    } else if (sort_engine == SortEngine::LOSER_TREE) {
        return std::make_unique<SortMergeReaderWithLoserTree>(
            std::move(record_readers), key_comparator_, user_defined_seq_comparator_,
            merge_function_wrapper);
    }
    return Status::Invalid("only support loser-tree or min-heap sort engine");
}

Result<bool> MergeFileSplitRead::Match(const std::shared_ptr<Split>& split,
                                       bool force_keep_delete) const {
    // TODO(yonghao.fyh): just pass split impl
    auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
    if (split_impl == nullptr) {
        return Status::Invalid("unexpected error, split cast to impl failed");
    }
    return split_impl->BeforeFiles().empty();
}

}  // namespace paimon
