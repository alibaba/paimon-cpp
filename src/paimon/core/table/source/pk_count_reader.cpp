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

#include "paimon/core/table/source/pk_count_reader.h"

#include <optional>
#include <unordered_map>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/type.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/mergetree/row_count_accumulator.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/merge_file_split_read.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/status.h"

namespace paimon {

PKCountReader::~PKCountReader() = default;

Result<std::unique_ptr<PKCountReader>> PKCountReader::Create(
    std::vector<std::shared_ptr<Split>> splits,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor) {
    const auto& table_schema = context->GetTableSchema();
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> pk_fields,
                           table_schema->TrimmedPrimaryKeyFields());
    std::shared_ptr<arrow::Schema> count_read_schema =
        DataField::ConvertDataFieldsToArrowSchema(pk_fields);

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> count_context,
                           InternalReadContext::CreateWithSchema(context, count_read_schema));

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<MergeFileSplitRead> merge_read,
        MergeFileSplitRead::Create(path_factory, count_context, memory_pool, executor));

    return std::unique_ptr<PKCountReader>(
        new PKCountReader(std::move(splits), count_context, std::move(merge_read), memory_pool));
}

Result<int64_t> PKCountReader::CountRows() {
    int64_t total = 0;
    for (const auto& split : splits_) {
        PAIMON_ASSIGN_OR_RAISE(int64_t split_count, CountSingleSplit(split));
        total += split_count;
    }
    return total;
}

PKCountReader::PKCountReader(std::vector<std::shared_ptr<Split>> splits,
                             const std::shared_ptr<InternalReadContext>& context,
                             std::unique_ptr<MergeFileSplitRead>&& merge_read,
                             const std::shared_ptr<MemoryPool>& memory_pool)
    : splits_(std::move(splits)),
      context_(context),
      merge_read_(std::move(merge_read)),
      pool_(memory_pool) {}

Result<int64_t> PKCountReader::CountSingleSplit(const std::shared_ptr<Split>& split) {
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split) {
        return Status::Invalid("split cannot be cast to DataSplitImpl");
    }

    if (data_split->DataFiles().empty()) {
        return 0;
    }

    if (data_split->RawConvertible()) {
        return MetadataCount(data_split);
    }

    return MergeCount(data_split);
}

Result<int64_t> PKCountReader::MetadataCount(const std::shared_ptr<DataSplitImpl>& split) {
    DeletionVector::Factory dv_factory = DeletionVector::CreateFactory(
        context_->GetCoreOptions().GetFileSystem(),
        DeletionVector::CreateDeletionFileMap(split->DataFiles(), split->DeletionFiles()), pool_);

    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> count, split->MergedRowCount(dv_factory));
    if (count.has_value()) {
        return count.value();
    }

    return Status::Invalid("not support split in pk count metadata fallback");
}

Result<int64_t> PKCountReader::MergeCount(const std::shared_ptr<DataSplitImpl>& split) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           merge_read_->GetPathFactory()->CreateDataFilePathFactory(
                               split->Partition(), split->Bucket()));

    auto dv_factory = DeletionVector::CreateFactory(
        context_->GetCoreOptions().GetFileSystem(),
        DeletionVector::CreateDeletionFileMap(split->DataFiles(), split->DeletionFiles()), pool_);

    std::vector<std::vector<SortedRun>> sections =
        IntervalPartition(split->DataFiles(), merge_read_->GetKeyComparator()).Partition();

    int64_t total_count = 0;

    for (const auto& section : sections) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader> merged_reader,
                               merge_read_->CreateSortMergeReaderForSection(
                                   section, split->Partition(), dv_factory,
                                   /*predicate=*/nullptr, data_file_path_factory,
                                   /*drop_delete=*/true));

        RowCountAccumulator accumulator(std::move(merged_reader));
        PAIMON_ASSIGN_OR_RAISE(int64_t section_count, accumulator.CountAll());
        total_count += section_count;
        accumulator.Close();
    }

    return total_count;
}

}  // namespace paimon
