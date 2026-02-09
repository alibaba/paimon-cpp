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

#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/c/abi.h"
#include "paimon/common/utils/threadsafe_queue.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/prefetch_file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/read_ahead_cache.h"
#include "paimon/utils/roaring_bitmap32.h"

struct ArrowSchema;

namespace paimon {

class ReaderBuilder;
class FileSystem;
class Executor;
class Predicate;
class Metrics;

class PrefetchFileBatchReaderImpl : public PrefetchFileBatchReader {
 public:
    static Result<std::unique_ptr<PrefetchFileBatchReaderImpl>> Create(
        const std::string& data_file_path, const ReaderBuilder* reader_builder,
        const std::shared_ptr<FileSystem>& fs, uint32_t prefetch_max_parallel_num,
        int32_t batch_size, uint32_t prefetch_batch_count, bool enable_adaptive_prefetch_strategy,
        const std::shared_ptr<Executor>& executor, bool initialize_read_ranges,
        PrefetchCacheMode prefetch_cache_mode, const CacheConfig& cache_config,
        const std::shared_ptr<MemoryPool>& pool);

    ~PrefetchFileBatchReaderImpl() override;

    Result<FileBatchReader::ReadBatch> NextBatch() override {
        return Status::Invalid(
            "paimon inner reader PrefetchFileBatchReader should use NextBatchWithBitmap");
    }
    Result<FileBatchReader::ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Status SeekToRow(uint64_t row_number) override;
    uint64_t GetPreviousBatchFirstRowNumber() const override;
    Result<uint64_t> GetNumberOfRows() const override;
    uint64_t GetNextRowToRead() const override;
    void Close() override;
    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) override;

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const override {
        assert(false);
        return Status::NotImplemented("gen read ranges not implemented");
    }
    bool SupportPreciseBitmapSelection() const override {
        return readers_[0]->SupportPreciseBitmapSelection();
    }

    Status RefreshReadRanges();

    inline PrefetchFileBatchReader* GetFirstReader() const {
        return readers_[0].get();
    }

    inline bool NeedPrefetch() const {
        return need_prefetch_;
    }

 private:
    struct PrefetchBatch {
        std::pair<uint64_t, uint64_t> read_range;
        BatchReader::ReadBatchWithBitmap batch;
        uint64_t previous_batch_first_row_num;
    };

    PrefetchFileBatchReaderImpl(
        const std::vector<std::shared_ptr<PrefetchFileBatchReader>>& readers, int32_t batch_size,
        uint32_t prefetch_queue_capacity, bool enable_adaptive_prefetch_strategy,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<ReadAheadCache>& cache,
        PrefetchCacheMode cache_mode);

    Status CleanUp();
    void Workloop();
    void SetReadStatus(const Status& status);
    Status GetReadStatus() const;
    Result<bool> IsEofRange(const std::pair<uint64_t, uint64_t>& read_range) const;
    Status DoReadBatch(size_t reader_idx);
    void ReadBatch(size_t reader_idx);
    size_t GetEnabledReaderSize() const;
    static std::vector<std::pair<uint64_t, uint64_t>> FilterReadRanges(
        const std::vector<std::pair<uint64_t, uint64_t>>& read_range,
        const std::optional<RoaringBitmap32>& selection_bitmap);

    static std::vector<std::vector<std::pair<uint64_t, uint64_t>>> DispatchReadRanges(
        const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges, size_t reader_count);

    Result<std::pair<uint64_t, uint64_t>> EofRange() const;
    std::optional<std::pair<uint64_t, uint64_t>> GetCurrentReadRange(size_t reader_idx) const;
    Status EnsureReaderPosition(size_t reader_idx,
                                const std::pair<uint64_t, uint64_t>& read_range) const;
    Status HandleReadResult(size_t reader_idx, const std::pair<uint64_t, uint64_t>& read_range,
                            FileBatchReader::ReadBatchWithBitmap&& read_batch_with_bitmap);
    bool NeedInitCache() const;

 private:
    std::vector<std::shared_ptr<PrefetchFileBatchReader>> readers_;
    // The meaning of readers_pos_ is: all data before this pos has been filtered out or effectively
    // consumed, and the data after this pos may need to be read in the next round of reading.
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> readers_pos_;
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> seek_cnt_;
    const int32_t batch_size_;
    std::optional<RoaringBitmap32> selection_bitmap_;
    std::shared_ptr<Predicate> predicate_;
    std::deque<std::pair<uint64_t, uint64_t>> read_ranges_;
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> read_ranges_in_group_;
    std::vector<std::unique_ptr<ThreadsafeQueue<PrefetchBatch>>> prefetch_queues_;
    std::vector<bool> reader_is_working_;
    std::mutex working_mutex_;
    std::condition_variable cv_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<ReadAheadCache> cache_;
    PrefetchCacheMode cache_mode_;

    mutable std::shared_mutex rw_mutex_;
    std::unique_ptr<std::thread> background_thread_;
    Status read_status_;
    std::atomic<bool> is_shutdown_ = false;
    uint64_t previous_batch_first_row_num_ = std::numeric_limits<uint64_t>::max();
    bool need_prefetch_ = false;
    bool read_ranges_freshed_ = false;
    const uint32_t prefetch_queue_capacity_;
    const bool enable_adaptive_prefetch_strategy_;
    int32_t parallel_num_;
};
}  // namespace paimon
