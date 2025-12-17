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

#include "paimon/core/operation/data_evolution_split_read.h"

#include "paimon/common/data/blob_utils.h"
#include "paimon/common/file_index/bitmap/apply_bitmap_index_batch_reader.h"
#include "paimon/common/global_index/complete_index_score_batch_reader.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/core/core_options.h"
#include "paimon/core/global_index/indexed_split_impl.h"

namespace paimon {
Status DataEvolutionSplitRead::BlobBunch::Add(const std::shared_ptr<DataFileMeta>& file) {
    if (!BlobUtils::IsBlobFile(file->file_name)) {
        return Status::Invalid("Only blob file can be added to a blob bunch.");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
    if (first_row_id == latest_first_row_id_) {
        if (file->max_sequence_number >= latest_max_sequence_number_) {
            return Status::Invalid(
                "Blob file with same first row id should have decreasing sequence number.");
        }
        // for files with the same first row id, file with larger sequence_number will be chosen,
        // other files will be skipped
        return Status::OK();
    }
    if (!files_.empty()) {
        if (has_row_ids_selection_) {
            // for the case:
            // snapshot 1: blob0 [0, 9]
            // snapshot 2: blob1 [0, 4] + blob2 [5, 9]
            // when selected row id is {5}, only blob0 and blob2 is reserved in scan process, as
            // blob1 has no intersect with {5}
            // BlobBunch will first add blob0 [0, 9]
            // then when it comes to blob2 [5, 9], blob0 will be removed as it has smaller sequence
            // number
            if (first_row_id < expected_next_first_row_id_) {
                if (file->max_sequence_number > latest_max_sequence_number_) {
                    row_count_ -= files_.back()->row_count;
                    files_.pop_back();
                } else {
                    return Status::OK();
                }
            }
        } else {
            if (first_row_id < expected_next_first_row_id_) {
                if (file->max_sequence_number >= latest_max_sequence_number_) {
                    return Status::Invalid(
                        "Blob file with overlapping row id should have decreasing sequence "
                        "number.");
                }
                // for files with overlapping, if the file with smaller sequence_number is chosen,
                // there will not exist file with larger sequence_number
                return Status::OK();
            } else if (first_row_id > expected_next_first_row_id_) {
                return Status::Invalid(
                    fmt::format("Blob file first row id should be continuous, expect {} but got {}",
                                expected_next_first_row_id_, first_row_id));
            }
        }
        if (!files_.empty()) {
            if (file->schema_id != files_[0]->schema_id) {
                return Status::Invalid("All files in a blob bunch should have the same schema id.");
            }
            if (file->write_cols != files_[0]->write_cols) {
                return Status::Invalid(
                    "All files in a blob bunch should have the same write columns.");
            }
        }
    }
    row_count_ += file->row_count;
    if (row_count_ > expected_row_count_) {
        return Status::Invalid(
            fmt::format("Blob files row count exceed the expect {}", expected_row_count_));
    }
    files_.push_back(file);
    latest_max_sequence_number_ = file->max_sequence_number;
    latest_first_row_id_ = first_row_id;
    expected_next_first_row_id_ = latest_first_row_id_ + file->row_count;
    return Status::OK();
}
DataEvolutionSplitRead::DataEvolutionSplitRead(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor)
    : AbstractSplitRead(path_factory, context,
                        std::make_unique<SchemaManager>(context->GetCoreOptions().GetFileSystem(),
                                                        context->GetPath(),
                                                        context->GetCoreOptions().GetBranch()),
                        memory_pool, executor) {}

bool DataEvolutionSplitRead::HasIndexScoreField(const std::shared_ptr<arrow::Schema>& read_schema) {
    return read_schema->GetFieldIndex(SpecialFields::IndexScore().Name()) != -1;
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(indexed_split->Validate());
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BatchReader> batch_reader,
            InnerCreateReader(indexed_split->GetDataSplit(), indexed_split->RowRanges()));
        if (HasIndexScoreField(raw_read_schema_)) {
            return std::make_unique<CompleteIndexScoreBatchReader>(std::move(batch_reader),
                                                                   indexed_split->Scores(), pool_);
        }
        return batch_reader;
    } else if (auto data_split = std::dynamic_pointer_cast<DataSplit>(split)) {
        if (HasIndexScoreField(raw_read_schema_)) {
            return Status::Invalid(
                "Invalid read schema, read _INDEX_SCORE while split cannot cast to IndexedSplit");
        }
        return InnerCreateReader(data_split, /*row_ranges=*/{});
    }
    return Status::Invalid("Invalid Split, cannot cast to IndexedSplit or DataSplit");
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::InnerCreateReader(
    const std::shared_ptr<DataSplit>& data_split, const std::vector<Range>& row_ranges) const {
    auto split_impl = dynamic_cast<DataSplitImpl*>(data_split.get());
    if (split_impl == nullptr) {
        return Status::Invalid("unexpected error, split cast to impl failed");
    }
    assert(raw_read_schema_->num_fields() > 0);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataFilePathFactory> data_file_path_factory,
        path_factory_->CreateDataFilePathFactory(split_impl->Partition(), split_impl->Bucket()));
    auto metas = split_impl->DataFiles();
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_by_row_id,
                           MergeRangesAndSort(std::move(metas)));

    std::vector<std::unique_ptr<BatchReader>> sub_readers;
    for (const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files : split_by_row_id) {
        if (need_merge_files.size() == 1) {
            // No need to merge fields, just create a single file reader
            PAIMON_ASSIGN_OR_RAISE(
                std::vector<std::unique_ptr<BatchReader>> raw_file_readers,
                CreateRawFileReaders(split_impl->Partition(), need_merge_files, raw_read_schema_,
                                     /*predicate=*/nullptr,
                                     /*deletion_file_map=*/{}, row_ranges, data_file_path_factory));
            assert(raw_file_readers.size() == 1);
            sub_readers.push_back(std::move(raw_file_readers[0]));
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DataEvolutionFileReader> evolution_reader,
                                   CreateUnionReader(split_impl->Partition(), need_merge_files,
                                                     row_ranges, data_file_path_factory));
            sub_readers.push_back(std::move(evolution_reader));
        }
    }
    auto concat_batch_reader = std::make_unique<ConcatBatchReader>(std::move(sub_readers), pool_);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BatchReader> batch_reader,
        ApplyPredicateFilterIfNeeded(std::move(concat_batch_reader), context_->GetPredicate()));
    return std::make_unique<CompleteRowKindBatchReader>(std::move(batch_reader), pool_);
}
Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::ApplyIndexAndDvReaderIfNeeded(
    std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
    const std::vector<Range>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    if (!deletion_file_map.empty()) {
        return Status::Invalid("DataEvolutionSplitRead do not support deletion vector");
    }
    if (predicate) {
        assert(false);
        // as DataEvolutionSplitRead will skip predicate
        return Status::Invalid("DataEvolutionSplitRead do not support predicate");
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap32> selection_row_ids,
                           file->ToFileSelection(row_ranges));
    ::ArrowSchema c_read_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_read_schema));
    PAIMON_RETURN_NOT_OK(
        file_reader->SetReadSchema(&c_read_schema, /*predicate=*/nullptr, selection_row_ids));

    std::unique_ptr<BatchReader> reader;
    if (!file_reader->SupportPreciseBitmapSelection() && selection_row_ids) {
        // several format(e.g. lance, blob) will return accurate batch result, where
        // ApplyBitmapIndexBatchReader is not necessary
        reader = std::make_unique<ApplyBitmapIndexBatchReader>(
            std::move(file_reader), std::move(selection_row_ids).value());
    } else {
        reader = std::move(file_reader);
    }

    return std::move(reader);
}

Result<std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>>>
DataEvolutionSplitRead::SplitFieldBunches(
    const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
    const std::function<Result<int32_t>(const std::shared_ptr<DataFileMeta>&)>&
        blob_field_to_field_id,
    bool has_row_ranges_selection) {
    std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> fields_files;
    std::map<int32_t, std::shared_ptr<DataEvolutionSplitRead::BlobBunch>> blob_bunch_map;
    int64_t row_count = -1;
    for (const auto& file : need_merge_files) {
        if (BlobUtils::IsBlobFile(file->file_name)) {
            PAIMON_ASSIGN_OR_RAISE(int32_t field_id, blob_field_to_field_id(file));
            auto iter = blob_bunch_map.find(field_id);
            if (iter == blob_bunch_map.end()) {
                auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
                    row_count, has_row_ranges_selection);
                PAIMON_RETURN_NOT_OK(blob_bunch->Add(file));
                blob_bunch_map.insert(std::make_pair(field_id, blob_bunch));
            } else {
                assert(iter->second);
                PAIMON_RETURN_NOT_OK(iter->second->Add(file));
            }
        } else {
            // Normal file, just add it to the current merge split
            fields_files.push_back(std::make_shared<DataEvolutionSplitRead::DataBunch>(file));
            row_count = file->row_count;
        }
    }
    for (const auto& [field_id, blob_bunch] : blob_bunch_map) {
        fields_files.push_back(blob_bunch);
    }
    return fields_files;
}

Result<std::unique_ptr<DataEvolutionFileReader>> DataEvolutionSplitRead::CreateUnionReader(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
    const std::vector<Range>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    auto blob_field_to_field_id =
        [&](const std::shared_ptr<DataFileMeta>& file_meta) -> Result<int32_t> {
        if (!BlobUtils::IsBlobFile(file_meta->file_name)) {
            return Status::Invalid("Only blob file need to call this method.");
        }
        auto data_schema = context_->GetTableSchema();
        if (data_schema->Id() != file_meta->schema_id) {
            PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(file_meta->schema_id));
        }
        const auto& write_cols = file_meta->write_cols;
        if (write_cols == std::nullopt || write_cols.value().size() != 1) {
            return Status::Invalid("Blob file only has one blob field");
        }
        PAIMON_ASSIGN_OR_RAISE(DataField data_field, data_schema->GetField(write_cols.value()[0]));
        return data_field.Id();
    };

    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> fields_files,
        SplitFieldBunches(need_merge_files, blob_field_to_field_id,
                          /*has_row_ranges_selection=*/!row_ranges.empty()));

    assert(!fields_files.empty());
    int64_t row_count = fields_files[0]->RowCount();
    PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, fields_files[0]->Files()[0]->NonNullFirstRowId());
    if (row_ranges.empty()) {
        for (const auto& bunch : fields_files) {
            if (bunch->RowCount() != row_count) {
                return Status::Invalid(
                    "All files in a field merge split should have the same row count.");
            }
            PAIMON_ASSIGN_OR_RAISE(int64_t current_bunch_first_row_id,
                                   bunch->Files()[0]->NonNullFirstRowId());
            if (current_bunch_first_row_id != first_row_id) {
                return Status::Invalid(
                    "All files in a field merge split should have the same first row id and could "
                    "not be null.");
            }
        }
    }

    // Init all we need to create a compound reader
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> all_read_fields,
                           DataField::ConvertArrowSchemaToDataFields(raw_read_schema_));
    std::vector<std::unique_ptr<BatchReader>> file_batch_readers(fields_files.size());
    std::vector<int32_t> read_field_ids = DataField::GetAllFieldIds(all_read_fields);
    // which row the read field index belongs to
    std::vector<int32_t> reader_offsets(all_read_fields.size(), -1);
    // which field index in the reading row
    std::vector<int32_t> field_offsets(all_read_fields.size(), -1);

    for (int32_t file_idx = 0; file_idx < static_cast<int32_t>(fields_files.size()); file_idx++) {
        const auto& bunch = fields_files[file_idx];
        const auto& first_file = bunch->Files()[0];
        int64_t schema_id = first_file->schema_id;
        std::shared_ptr<TableSchema> data_schema = context_->GetTableSchema();
        if (schema_id != data_schema->Id()) {
            PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(schema_id));
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<DataField> data_fields,
            ProjectFieldsForRowTrackingAndDataEvolution(data_schema, first_file->write_cols));
        std::vector<int32_t> data_field_ids = DataField::GetAllFieldIds(data_fields);
        std::vector<DataField> read_fields_in_file;
        for (int32_t read_field_idx = 0;
             read_field_idx < static_cast<int32_t>(read_field_ids.size()); read_field_idx++) {
            // -1 indicates that the read fields are not matched
            if (reader_offsets[read_field_idx] != -1) {
                continue;
            }
            for (int32_t data_field_id : data_field_ids) {
                // Check if the read field index matches the file field
                // index
                if (data_field_id == read_field_ids[read_field_idx]) {
                    // "file_idx" is the reader index, and "read_fields_in_file.size()"
                    // is the offset the that row
                    reader_offsets[read_field_idx] = file_idx;
                    field_offsets[read_field_idx] = read_fields_in_file.size();
                    read_fields_in_file.push_back(all_read_fields[read_field_idx]);
                    break;
                }
            }
        }

        if (!read_fields_in_file.empty()) {
            // create new FieldMappingReader for read partial fields
            auto file_read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields_in_file);
            PAIMON_ASSIGN_OR_RAISE(
                std::vector<std::unique_ptr<BatchReader>> file_readers,
                CreateRawFileReaders(partition, bunch->Files(), file_read_schema,
                                     /*predicate=*/nullptr, /*deletion_file_map=*/{}, row_ranges,
                                     data_file_path_factory));
            if (file_readers.size() == 1) {
                file_batch_readers[file_idx] = std::move(file_readers[0]);
            } else {
                // Concat multiple blob files that map to the same data file.
                file_batch_readers[file_idx] =
                    std::make_unique<ConcatBatchReader>(std::move(file_readers), pool_);
            }
        }
    }
    // TODO(xinyu.lxy): check nullable when reader_offsets[read_field_idx] = -1
    return DataEvolutionFileReader::Create(std::move(file_batch_readers), raw_read_schema_,
                                           options_.GetReadBatchSize(), reader_offsets,
                                           field_offsets, pool_);
}

Result<bool> DataEvolutionSplitRead::Match(const std::shared_ptr<Split>& split,
                                           bool force_keep_delete) const {
    return true;
}

Result<std::vector<std::vector<std::shared_ptr<DataFileMeta>>>>
DataEvolutionSplitRead::MergeRangesAndSort(std::vector<std::shared_ptr<DataFileMeta>>&& files) {
    // group by row id range
    RangeHelper<std::shared_ptr<DataFileMeta>> range_helper(
        [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int64_t> {
            return meta->NonNullFirstRowId();
        },
        [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int64_t> {
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, meta->NonNullFirstRowId());
            return first_row_id + meta->row_count - 1;
        });

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> result,
                           range_helper.MergeOverlappingRanges(std::move(files)));
    // in group, sort by blob file and max_seq
    for (auto& group : result) {
        // split to data files and blob files
        std::vector<std::shared_ptr<DataFileMeta>> data_files;
        data_files.reserve(group.size());
        std::vector<std::shared_ptr<DataFileMeta>> blob_files;
        blob_files.reserve(group.size());
        for (auto& meta : group) {
            if (BlobUtils::IsBlobFile(meta->file_name)) {
                blob_files.push_back(std::move(meta));
            } else {
                data_files.push_back(std::move(meta));
            }
        }

        // data files sort by reversed max sequence number
        std::stable_sort(
            data_files.begin(), data_files.end(),
            [](const std::shared_ptr<DataFileMeta>& m1, const std::shared_ptr<DataFileMeta>& m2) {
                return m1->max_sequence_number > m2->max_sequence_number;
            });

        PAIMON_ASSIGN_OR_RAISE(bool all_range_same, range_helper.AreAllRangesSame(data_files));
        if (!all_range_same) {
            return Status::Invalid("Data files should be all row id ranges same.");
        }

        // blob files sort by first row id then by reversed max sequence number
        std::stable_sort(
            blob_files.begin(), blob_files.end(),
            [](const std::shared_ptr<DataFileMeta>& m1, const std::shared_ptr<DataFileMeta>& m2) {
                if (m1->first_row_id.value() != m2->first_row_id.value()) {
                    return m1->first_row_id.value() < m2->first_row_id.value();
                }
                return m1->max_sequence_number > m2->max_sequence_number;
            });
        // concat data files and blob files
        group.clear();
        group.insert(group.end(), std::make_move_iterator(data_files.begin()),
                     std::make_move_iterator(data_files.end()));
        group.insert(group.end(), std::make_move_iterator(blob_files.begin()),
                     std::make_move_iterator(blob_files.end()));
    }
    return result;
}
}  // namespace paimon
