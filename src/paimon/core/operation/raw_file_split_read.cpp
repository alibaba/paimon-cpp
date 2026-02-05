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

#include "paimon/core/operation/raw_file_split_read.h"

#include <optional>
#include <utility>
#include <vector>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/file_index/bitmap/apply_bitmap_index_batch_reader.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class DataFilePathFactory;
class Executor;
class Predicate;
struct DeletionFile;

RawFileSplitRead::RawFileSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                   const std::shared_ptr<InternalReadContext>& context,
                                   const std::shared_ptr<MemoryPool>& memory_pool,
                                   const std::shared_ptr<Executor>& executor)
    : AbstractSplitRead(path_factory, context,
                        std::make_unique<SchemaManager>(context->GetCoreOptions().GetFileSystem(),
                                                        context->GetPath(),
                                                        context->GetCoreOptions().GetBranch()),
                        memory_pool, executor) {}

Result<std::unique_ptr<BatchReader>> RawFileSplitRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split) {
        return Status::Invalid("cannot cast split to data_split in RawFileSplitRead");
    }
    auto deletion_file_map = CreateDeletionFileMap(*data_split);
    const auto& predicate = context_->GetPredicate();
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataFilePathFactory> data_file_path_factory,
        path_factory_->CreateDataFilePathFactory(data_split->Partition(), data_split->Bucket()));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> raw_file_readers,
                           CreateRawFileReaders(data_split->Partition(), data_split->DataFiles(),
                                                raw_read_schema_, predicate, deletion_file_map,
                                                /*row_ranges=*/{}, data_file_path_factory));
    auto concat_batch_reader =
        std::make_unique<ConcatBatchReader>(std::move(raw_file_readers), pool_);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> batch_reader,
                           ApplyPredicateFilterIfNeeded(std::move(concat_batch_reader), predicate));
    return std::make_unique<CompleteRowKindBatchReader>(std::move(batch_reader), pool_);
}

Result<bool> RawFileSplitRead::Match(const std::shared_ptr<Split>& split,
                                     bool force_keep_delete) const {
    auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
    if (split_impl == nullptr) {
        return Status::Invalid("unexpected error, split cast to impl failed");
    }
    if (context_->GetTableSchema()->PrimaryKeys().empty()) {
        // for append table, always return true
        return true;
    }
    bool matched = !force_keep_delete && !split_impl->IsStreaming() && split_impl->RawConvertible();
    if (matched) {
        // for legacy version, we are not sure if there are delete rows, but in order to be
        // compatible with the query acceleration of the OLAP engine, we have generated raw
        // files.
        // Here, for the sake of correctness, we still need to perform drop delete filtering.
        for (const auto& file : split_impl->DataFiles()) {
            if (file->delete_row_count == std::nullopt) {
                return false;
            }
        }
    }
    return matched;
}

Result<std::unique_ptr<BatchReader>> RawFileSplitRead::ApplyIndexAndDvReaderIfNeeded(
    std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
    const std::optional<std::vector<Range>>& ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    std::shared_ptr<FileIndexResult> file_index_result;
    if (options_.FileIndexReadEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(
            file_index_result,
            FileIndexEvaluator::Evaluate(data_schema, predicate, data_file_path_factory, file,
                                         options_.GetFileSystem(), pool_));
        PAIMON_ASSIGN_OR_RAISE(bool is_remain, file_index_result->IsRemain());
        if (!is_remain) {
            return std::unique_ptr<FileBatchReader>();
        }
    }
    // prepare selection bitmap for index
    const RoaringBitmap32* selection = nullptr;
    if (auto* bitmap_file_index = dynamic_cast<BitmapIndexResult*>(file_index_result.get())) {
        PAIMON_ASSIGN_OR_RAISE(selection, bitmap_file_index->GetBitmap());
    }

    // prepare deletion bitmap for deletion vector
    PAIMON_UNIQUE_PTR<DeletionVector> deletion_vector;
    auto dv_iter = deletion_file_map.find(file->file_name);
    if (dv_iter != deletion_file_map.end()) {
        PAIMON_ASSIGN_OR_RAISE(deletion_vector, DeletionVector::Read(options_.GetFileSystem().get(),
                                                                     dv_iter->second, pool_.get()));
    }
    const RoaringBitmap32* deletion = nullptr;
    if (auto* bitmap_dv = dynamic_cast<BitmapDeletionVector*>(deletion_vector.get())) {
        deletion = bitmap_dv->GetBitmap();
    }

    // merge deletion and bitmap index selection
    std::optional<RoaringBitmap32> actual_selection;
    if (selection && deletion) {
        actual_selection = RoaringBitmap32::AndNot(*selection, *deletion);
    } else if (selection) {
        actual_selection = *selection;
    } else if (deletion) {
        actual_selection = *deletion;
        PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, file_reader->GetNumberOfRows());
        actual_selection.value().Flip(0, num_rows);
    }

    if (actual_selection && actual_selection.value().IsEmpty()) {
        return std::unique_ptr<FileBatchReader>();
    }

    ::ArrowSchema c_read_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_read_schema));
    PAIMON_RETURN_NOT_OK(file_reader->SetReadSchema(&c_read_schema, predicate, actual_selection));

    std::unique_ptr<BatchReader> reader;
    if (!file_reader->SupportPreciseBitmapSelection() && actual_selection) {
        // several format(e.g. lance, blob) will return accurate batch result, where
        // ApplyBitmapIndexBatchReader is not necessary
        reader = std::make_unique<ApplyBitmapIndexBatchReader>(std::move(file_reader),
                                                               std::move(actual_selection).value());
    } else {
        reader = std::move(file_reader);
    }

    if (deletion_vector && !deletion && !deletion_vector->IsEmpty()) {
        // TODO(xinyu.lxy): if deletion vector is bitmap, use ApplyBitmapIndexBatchReader to
        // filter result
        return Status::NotImplemented("Only support BitmapDeletionVector");
    }
    return std::move(reader);
}

}  // namespace paimon
