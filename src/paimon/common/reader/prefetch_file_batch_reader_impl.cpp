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

#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <thread>

#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/io/cache_input_stream.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/utils/read_ahead_cache.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

Result<std::unique_ptr<PrefetchFileBatchReaderImpl>> PrefetchFileBatchReaderImpl::Create(
    const std::string& data_file_path, const ReaderBuilder* reader_builder,
    const std::shared_ptr<FileSystem>& fs, uint32_t prefetch_max_parallel_num, int32_t batch_size,
    uint32_t prefetch_batch_count, bool enable_adaptive_prefetch_strategy,
    const std::shared_ptr<Executor>& executor, bool initialize_read_ranges,
    bool enable_prefetch_cache, const CacheConfig& cache_config,
    const std::shared_ptr<MemoryPool>& pool) {
    if (prefetch_max_parallel_num == 0) {
        return Status::Invalid("prefetch max parallel num should be greater than 0.");
    }
    if (prefetch_batch_count == 0) {
        return Status::Invalid("prefetch batch count should be greater than 0.");
    }
    if (batch_size <= 0) {
        return Status::Invalid("batch size should be greater than 0.");
    }
    if (reader_builder == nullptr) {
        return Status::Invalid("reader_builder should not be nullptr.");
    }
    if (fs == nullptr) {
        return Status::Invalid("file system should not be nullptr.");
    }
    if (executor == nullptr) {
        return Status::Invalid("executor should not be nullptr.");
    }

    std::shared_ptr<ReadAheadCache> cache;
    if (enable_prefetch_cache) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream, fs->Open(data_file_path));
        cache = std::make_shared<ReadAheadCache>(input_stream, cache_config, pool);
    }
    std::vector<std::future<Result<std::unique_ptr<FileBatchReader>>>> futures;
    for (uint32_t i = 0; i < prefetch_max_parallel_num; i++) {
        futures.push_back(Via(executor.get(),
                              [&fs, &data_file_path, &reader_builder,
                               &cache]() -> Result<std::unique_ptr<FileBatchReader>> {
                                  PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input_stream,
                                                         fs->Open(data_file_path));
                                  auto cache_input_stream = std::make_shared<CacheInputStream>(
                                      std::move(input_stream), cache);
                                  return reader_builder->Build(cache_input_stream);
                              }));
    }
    std::vector<std::shared_ptr<PrefetchFileBatchReader>> readers;
    for (auto& file_batch_reader : CollectAll(futures)) {
        if (!file_batch_reader.ok()) {
            return file_batch_reader.status();
        }
        std::shared_ptr<FileBatchReader> reader = std::move(file_batch_reader).value();
        auto prefetch_file_batch_reader =
            std::dynamic_pointer_cast<PrefetchFileBatchReader>(reader);
        if (prefetch_file_batch_reader == nullptr) {
            return Status::Invalid(
                "failed to cast to prefetch file batch reader. file format not support prefetch");
        }
        readers.emplace_back(prefetch_file_batch_reader);
    }
    if (prefetch_batch_count < readers.size()) {
        prefetch_batch_count = readers.size();
    }
    uint32_t prefetch_queue_capacity = prefetch_batch_count / readers.size();

    auto reader = std::unique_ptr<PrefetchFileBatchReaderImpl>(
        new PrefetchFileBatchReaderImpl(readers, batch_size, prefetch_queue_capacity,
                                        enable_adaptive_prefetch_strategy, executor, cache));
    if (initialize_read_ranges) {
        // normally initialize read ranges should be false, as set read schema will refresh read
        // ranges, and set read schema will always be called before read.
        PAIMON_RETURN_NOT_OK(reader->RefreshReadRanges());
    }
    return reader;
}

PrefetchFileBatchReaderImpl::PrefetchFileBatchReaderImpl(
    const std::vector<std::shared_ptr<PrefetchFileBatchReader>>& readers, int32_t batch_size,
    uint32_t prefetch_queue_capacity, bool enable_adaptive_prefetch_strategy,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<ReadAheadCache>& cache)
    : readers_(std::move(readers)),
      batch_size_(batch_size),
      executor_(executor),
      cache_(cache),
      prefetch_queue_capacity_(prefetch_queue_capacity),
      enable_adaptive_prefetch_strategy_(enable_adaptive_prefetch_strategy) {
    for (size_t i = 0; i < readers_.size(); i++) {
        prefetch_queues_.emplace_back(std::make_unique<ThreadsafeQueue<PrefetchBatch>>());
        readers_pos_.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
        reader_is_working_.emplace_back(false);
    }
    parallel_num_ = readers_.size();
}

PrefetchFileBatchReaderImpl::~PrefetchFileBatchReaderImpl() {
    (void)CleanUp();
}

Status PrefetchFileBatchReaderImpl::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(read_schema));
    for (const auto& reader : readers_) {
        auto c_schema = std::make_unique<::ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, c_schema.get()));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(c_schema.get(), predicate, selection_bitmap));
    }
    selection_bitmap_ = selection_bitmap;
    return RefreshReadRanges();
}

Status PrefetchFileBatchReaderImpl::RefreshReadRanges() {
    PAIMON_RETURN_NOT_OK(CleanUp());
    bool need_prefetch;
    PAIMON_ASSIGN_OR_RAISE(auto read_ranges, readers_[0]->GenReadRanges(&need_prefetch));

    if (!enable_adaptive_prefetch_strategy_) {
        need_prefetch = true;
    } else if (need_prefetch && enable_adaptive_prefetch_strategy_ && !read_ranges.empty()) {
        uint64_t batch_count_in_range =
            (read_ranges[0].second - read_ranges[0].first) / batch_size_;
        if (batch_count_in_range > static_cast<uint64_t>(prefetch_queue_capacity_)) {
            need_prefetch = false;
        }
    }

    need_prefetch_ = need_prefetch;
    PAIMON_RETURN_NOT_OK(SetReadRanges(FilterReadRanges(read_ranges, selection_bitmap_)));
    read_ranges_freshed_ = true;

    return Status::OK();
}

std::vector<std::pair<uint64_t, uint64_t>> PrefetchFileBatchReaderImpl::FilterReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!selection_bitmap) {
        return read_ranges;
    }
    std::vector<std::pair<uint64_t, uint64_t>> result;
    for (const auto& read_range : read_ranges) {
        if (selection_bitmap.value().ContainsAny(read_range.first, read_range.second)) {
            result.push_back(read_range);
        }
    }
    return result;
}

Status PrefetchFileBatchReaderImpl::SetReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) {
    // push down read ranges for reducing IO amplification
    read_ranges_in_group_ = DispatchReadRanges(read_ranges, readers_.size());
    if (need_prefetch_ && readers_.size() > 1) {
        // if prefetching isn't necessary, then setting read ranges won't be needed either.
        std::vector<std::future<Status>> futures;
        for (size_t i = 0; i < readers_.size(); i++) {
            futures.push_back(Via(executor_.get(), [this, i]() -> Status {
                return readers_[i]->SetReadRanges(read_ranges_in_group_[i]);
            }));
        }
        for (const auto& status : CollectAll(futures)) {
            if (!status.ok()) {
                return status;
            }
        }
    }
    for (const auto& read_range : read_ranges) {
        read_ranges_.push_back(read_range);
    }
    // Note: add a special read range out of file row count, for trigger an EOF access.
    std::pair<uint64_t, uint64_t> eof_range;
    PAIMON_ASSIGN_OR_RAISE(eof_range, EofRange());
    read_ranges_.push_back(eof_range);
    for (auto& read_ranges : read_ranges_in_group_) {
        read_ranges.push_back(eof_range);
    }
    return Status::OK();
}

std::vector<std::vector<std::pair<uint64_t, uint64_t>>>
PrefetchFileBatchReaderImpl::DispatchReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges, size_t group_count) {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> read_ranges_in_group;
    read_ranges_in_group.resize(group_count);
    for (size_t i = 0; i < read_ranges.size(); i++) {
        read_ranges_in_group[i % group_count].push_back(read_ranges[i]);
    }
    return read_ranges_in_group;
}

Status PrefetchFileBatchReaderImpl::CleanUp() {
    auto clean_prefetch_queue = [this]() {
        for (auto& prefetch_queue : prefetch_queues_) {
            while (true) {
                std::optional<PrefetchBatch> batch = prefetch_queue->try_pop();
                {
                    std::unique_lock<std::mutex> lock(working_mutex_);
                    cv_.notify_one();
                }
                if (batch == std::nullopt) {
                    break;
                }
                ReaderUtils::ReleaseReadBatch(std::move(batch.value().batch.first));
            }
        }
    };
    // Clear the existing read ranges and prefetch queue
    {
        std::unique_lock<std::mutex> lock(working_mutex_);
        is_shutdown_ = true;  // set is shutdown and check shutdown to avoid block at queue.push
        cv_.notify_one();
    }
    // Join and reset the background thread if it exists
    if (background_thread_) {
        if (background_thread_->joinable()) {
            background_thread_->join();
            background_thread_.reset();
        } else {
            return Status::Invalid("background thread is not joinable");
        }
    }

    read_ranges_.clear();
    read_ranges_in_group_.clear();
    clean_prefetch_queue();
    for (size_t i = 0; i < readers_pos_.size(); i++) {
        readers_pos_[i]->store(0);
        reader_is_working_[i] = false;
    }
    is_shutdown_ = false;
    if (cache_) {
        cache_->Reset();
    }
    SetReadStatus(Status::OK());
    return Status::OK();
}

void PrefetchFileBatchReaderImpl::Workloop() {
    std::vector<std::future<void>> futures;
    futures.resize(readers_.size());
    if (cache_) {
        auto read_ranges = readers_[0]->PreBufferRange();
        if (read_ranges.ok()) {
            std::vector<ByteRange> ranges;
            for (const auto& read_range : read_ranges.value()) {
                ranges.emplace_back(read_range.first, read_range.second);
            }
            auto s = cache_->Init(std::move(ranges));
            if (!s.ok()) {
                SetReadStatus(s);
                return;
            }
        } else {
            SetReadStatus(read_ranges.status());
            return;
        }
    }

    while (true) {
        if (!GetReadStatus().ok()) {
            break;
        }
        if (is_shutdown_) {
            break;
        }
        bool all_finished = true;
        for (const auto& reader_pos : readers_pos_) {
            if (reader_pos->load() != std::numeric_limits<uint64_t>::max()) {
                all_finished = false;
            }
        }
        if (all_finished) {
            break;
        }

        bool made_progress_this_iteration = false;
        for (size_t reader_idx = 0; reader_idx < readers_.size(); reader_idx++) {
            if (!futures[reader_idx].valid() ||
                (futures[reader_idx].wait_for(std::chrono::microseconds(0)) ==
                 std::future_status::ready)) {
                if (futures[reader_idx].valid()) {
                    futures[reader_idx].get();
                }
                if (prefetch_queues_[reader_idx]->size() >= prefetch_queue_capacity_) {
                    // queue is full, skip
                    continue;
                }
                if (readers_pos_[reader_idx]->load() != std::numeric_limits<uint64_t>::max()) {
                    futures[reader_idx] =
                        Via(executor_.get(), [this, reader_idx]() { ReadBatch(reader_idx); });
                    made_progress_this_iteration = true;
                }
            }
        }
        if (!made_progress_this_iteration) {
            std::unique_lock<std::mutex> lock(working_mutex_);
            cv_.wait(lock, [this] {
                if (is_shutdown_) {
                    return true;
                }
                for (size_t i = 0; i < reader_is_working_.size(); i++) {
                    if (reader_is_working_[i]) {
                        continue;
                    }
                    if (prefetch_queues_[i]->size() >= prefetch_queue_capacity_) {
                        continue;
                    }
                    if (readers_pos_[i]->load() == std::numeric_limits<uint64_t>::max()) {
                        continue;
                    }
                    return true;
                }
                return false;
            });
        }
    }
    Wait(futures);
}

void PrefetchFileBatchReaderImpl::ReadBatch(size_t reader_idx) {
    Status status = DoReadBatch(reader_idx);
    if (!status.ok()) {
        SetReadStatus(status);
    }
}

std::optional<std::pair<uint64_t, uint64_t>> PrefetchFileBatchReaderImpl::GetCurrentReadRange(
    size_t reader_idx) const {
    const auto& read_ranges = read_ranges_in_group_[reader_idx];
    const auto& current_pos = readers_pos_[reader_idx];
    uint64_t current_pos_value = current_pos->load();

    for (const auto& range : read_ranges) {
        if (current_pos_value < range.second) {
            return range;
        }
    }
    return std::nullopt;
}

Status PrefetchFileBatchReaderImpl::EnsureReaderPosition(
    size_t reader_idx, const std::pair<uint64_t, uint64_t>& current_read_range) const {
    uint64_t pos = std::max(readers_pos_[reader_idx]->load(), current_read_range.first);
    if (readers_[reader_idx]->GetNextRowToRead() != pos) {
        return readers_[reader_idx]->SeekToRow(pos);
    }
    return Status::OK();
}

Status PrefetchFileBatchReaderImpl::HandleReadResult(
    size_t reader_idx, const std::pair<uint64_t, uint64_t>& read_range,
    ReadBatchWithBitmap&& read_batch_with_bitmap) {
    uint64_t first_row_number = readers_[reader_idx]->GetPreviousBatchFirstRowNumber();
    auto& prefetch_queue = prefetch_queues_[reader_idx];
    if (!BatchReader::IsEofBatch(read_batch_with_bitmap)) {
        auto& [read_batch, bitmap] = read_batch_with_bitmap;
        auto& [c_array, c_schema] = read_batch;

        if (first_row_number >= read_range.second) {
            // fully out of range, data before first_row_number has been filtered out
            readers_pos_[reader_idx]->store(first_row_number);
            ReaderUtils::ReleaseReadBatch(std::move(read_batch));
            return Status::OK();
        } else if (first_row_number + c_array->length > read_range.second) {
            // partially out of range, data before read_range.second has been effectively consumed
            readers_pos_[reader_idx]->store(read_range.second);
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> src_array,
                                              arrow::ImportArray(c_array.get(), c_schema.get()));
            int32_t target_length = read_range.second - first_row_number;
            auto array = src_array->Slice(/*offset=*/0, target_length);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                arrow::ExportArray(*array, c_array.get(), c_schema.get()));
            bitmap.RemoveRange(target_length, src_array->length());
        } else {
            // all within the range, data before readers_[reader_idx]->GetNextRowToRead() has been
            // effectively consumed
            readers_pos_[reader_idx]->store(readers_[reader_idx]->GetNextRowToRead());
        }
        if (bitmap.IsEmpty()) {
            ReaderUtils::ReleaseReadBatch(std::move(read_batch));
            return Status::OK();
        }
        prefetch_queue->push({read_range, std::move(read_batch_with_bitmap), first_row_number});
    } else {
        std::pair<uint64_t, uint64_t> eof_range;
        PAIMON_ASSIGN_OR_RAISE(eof_range, EofRange());
        prefetch_queue->push({eof_range, std::move(read_batch_with_bitmap), first_row_number});
        readers_pos_[reader_idx]->store(std::numeric_limits<uint64_t>::max());
    }
    return Status::OK();
}

Status PrefetchFileBatchReaderImpl::DoReadBatch(size_t reader_idx) {
    PAIMON_RETURN_NOT_OK(GetReadStatus());
    if (is_shutdown_) {
        return Status::OK();
    }
    std::optional<std::pair<uint64_t, uint64_t>> current_read_range =
        GetCurrentReadRange(reader_idx);
    if (current_read_range == std::nullopt) {
        // No more read ranges for this reader, gracefully exit.
        return Status::OK();
    }
    ScopeGuard guard([&]() {
        std::unique_lock<std::mutex> lock(working_mutex_);
        reader_is_working_[reader_idx] = false;
        cv_.notify_one();
    });
    {
        std::unique_lock<std::mutex> lock(working_mutex_);
        reader_is_working_[reader_idx] = true;
    }

    const auto& read_range = current_read_range.value();
    FileBatchReader* reader = readers_[reader_idx].get();
    PAIMON_RETURN_NOT_OK(EnsureReaderPosition(reader_idx, read_range));

    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap read_batch_with_bitmap,
                           reader->NextBatchWithBitmap());

    return HandleReadResult(reader_idx, read_range, std::move(read_batch_with_bitmap));
}

Result<BatchReader::ReadBatchWithBitmap> PrefetchFileBatchReaderImpl::NextBatchWithBitmap() {
    if (!read_ranges_freshed_) {
        return Status::Invalid("prefetch reader read ranges are not initialized");
    }
    if (!background_thread_) {
        background_thread_ =
            std::make_unique<std::thread>(&PrefetchFileBatchReaderImpl::Workloop, this);
    }

    while (true) {
        PAIMON_RETURN_NOT_OK(GetReadStatus());
        if (is_shutdown_) {
            return Status::Invalid(
                "prefetch reader has inconsistent state, maybe read while closing reader or change "
                "read schema");
        }
        std::optional<std::pair<uint64_t, uint64_t>> min_range;
        size_t eof_count = 0;
        size_t value_count = 0;
        for (auto& prefetch_queue : prefetch_queues_) {
            PAIMON_RETURN_NOT_OK(GetReadStatus());
            const PrefetchBatch* peek_batch = prefetch_queue->try_front();
            if (!peek_batch) {
                continue;
            }
            if (min_range == std::nullopt) {
                min_range = peek_batch->read_range;
            } else {
                if (peek_batch->read_range.first < min_range.value().first) {
                    min_range = peek_batch->read_range;
                }
            }
            value_count++;
            PAIMON_ASSIGN_OR_RAISE(bool is_eof_range, IsEofRange(peek_batch->read_range));
            if (is_eof_range) {
                eof_count++;
                continue;
            }

            const auto& current_read_range = read_ranges_.front();
            if (peek_batch->read_range == current_read_range) {
                auto prefetch_batch = prefetch_queue->try_pop();
                {
                    std::unique_lock<std::mutex> lock(working_mutex_);
                    cv_.notify_one();
                }
                previous_batch_first_row_num_ = prefetch_batch.value().previous_batch_first_row_num;
                return std::move(prefetch_batch).value().batch;
            }
        }
        if (eof_count == prefetch_queues_.size()) {
            const PrefetchBatch* peek_batch = prefetch_queues_[0]->try_front();
            if (peek_batch == nullptr) {
                assert(false);
                return Status::Invalid("peek batch not suppose to be nullptr");
            }
            previous_batch_first_row_num_ = peek_batch->previous_batch_first_row_num;
            return BatchReader::MakeEofBatchWithBitmap();
        }
        if (value_count == prefetch_queues_.size()) {
            while (true) {
                if (read_ranges_.empty()) {
                    break;
                }
                const auto& current_read_range = read_ranges_.front();
                if (current_read_range.first < min_range.value().first) {
                    read_ranges_.pop_front();
                } else {
                    break;
                }
            }
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }
}

Status PrefetchFileBatchReaderImpl::SeekToRow(uint64_t row_number) {
    return Status::NotImplemented("not support seek to row for prefetch reader");
}

std::shared_ptr<Metrics> PrefetchFileBatchReaderImpl::GetReaderMetrics() const {
    return MetricsImpl::CollectReadMetrics(readers_);
}

Result<std::unique_ptr<::ArrowSchema>> PrefetchFileBatchReaderImpl::GetFileSchema() const {
    assert(!readers_.empty());
    return readers_[0]->GetFileSchema();
}

uint64_t PrefetchFileBatchReaderImpl::GetPreviousBatchFirstRowNumber() const {
    return previous_batch_first_row_num_;
}

Result<uint64_t> PrefetchFileBatchReaderImpl::GetNumberOfRows() const {
    assert(!readers_.empty());
    return readers_[0]->GetNumberOfRows();
}

uint64_t PrefetchFileBatchReaderImpl::GetNextRowToRead() const {
    assert(false);
    return -1;
}

void PrefetchFileBatchReaderImpl::SetReadStatus(const Status& status) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    read_status_ = status;
}

Status PrefetchFileBatchReaderImpl::GetReadStatus() const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    return read_status_;
}
Result<bool> PrefetchFileBatchReaderImpl::IsEofRange(
    const std::pair<uint64_t, uint64_t>& read_range) const {
    PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, GetNumberOfRows());
    return read_range.first >= num_rows;
}

Result<std::pair<uint64_t, uint64_t>> PrefetchFileBatchReaderImpl::EofRange() const {
    PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, GetNumberOfRows());
    return std::make_pair(num_rows, num_rows + 1);
}

void PrefetchFileBatchReaderImpl::Close() {
    (void)CleanUp();
    for (const auto& reader : readers_) {
        reader->Close();
    }
}

}  // namespace paimon
