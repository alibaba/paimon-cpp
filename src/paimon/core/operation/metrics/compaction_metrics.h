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

#pragma once

#include <algorithm>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/metrics/metrics_impl.h"

namespace paimon {
/// Metrics to measure a compaction.
class CompactionMetrics {
 public:
    static constexpr int32_t kCompactionTimeWindow = 100;

    static constexpr char MAX_LEVEL0_FILE_COUNT[] = "maxLevel0FileCount";
    static constexpr char AVG_LEVEL0_FILE_COUNT[] = "avgLevel0FileCount";
    static constexpr char AVG_COMPACTION_TIME[] = "avgCompactionTime";
    static constexpr char COMPACTION_COMPLETED_COUNT[] = "compactionCompletedCount";
    static constexpr char COMPACTION_TOTAL_COUNT[] = "compactionTotalCount";
    static constexpr char COMPACTION_QUEUED_COUNT[] = "compactionQueuedCount";
    static constexpr char MAX_COMPACTION_INPUT_SIZE[] = "maxCompactionInputSize";
    static constexpr char MAX_COMPACTION_OUTPUT_SIZE[] = "maxCompactionOutputSize";
    static constexpr char AVG_COMPACTION_INPUT_SIZE[] = "avgCompactionInputSize";
    static constexpr char AVG_COMPACTION_OUTPUT_SIZE[] = "avgCompactionOutputSize";
    static constexpr char MAX_TOTAL_FILE_SIZE[] = "maxTotalFileSize";
    static constexpr char AVG_TOTAL_FILE_SIZE[] = "avgTotalFileSize";

    class Reporter {
     public:
        Reporter(CompactionMetrics* metrics, const BinaryRow& partition, int32_t bucket)
            : metrics_(metrics), partition_(partition), bucket_(bucket) {}

        void ReportLevel0FileCount(int64_t count) {
            level0_file_count_ = count;
        }
        void ReportCompactionInputSize(int64_t bytes) {
            compaction_input_size_ = bytes;
        }
        void ReportCompactionOutputSize(int64_t bytes) {
            compaction_output_size_ = bytes;
        }
        void ReportTotalFileSize(int64_t bytes) {
            total_file_size_ = bytes;
        }
        void ReportCompactionTime(int64_t time) {
            metrics_->ReportCompactionTime(time);
        }

        void IncreaseCompactionsCompletedCount() {
            metrics_->IncreaseCompactionsCompletedCount();
        }
        void IncreaseCompactionsTotalCount() {
            metrics_->IncreaseCompactionsTotalCount();
        }
        void IncreaseCompactionsQueuedCount() {
            metrics_->IncreaseCompactionsQueuedCount();
        }
        void DecreaseCompactionsQueuedCount() {
            metrics_->DecreaseCompactionsQueuedCount();
        }
        void Unregister() {
            metrics_->EraseReporter(partition_, bucket_);
        }

        int64_t Level0FileCount() const {
            return level0_file_count_;
        }

        int64_t CompactionInputSize() const {
            return compaction_input_size_;
        }

        int64_t CompactionOutputSize() const {
            return compaction_output_size_;
        }

        int64_t TotalFileSize() const {
            return total_file_size_;
        }

     private:
        CompactionMetrics* metrics_;
        BinaryRow partition_;
        int32_t bucket_;

        // Data fields for metrics.
        int64_t level0_file_count_ = 0;
        int64_t compaction_input_size_ = 0;
        int64_t compaction_output_size_ = 0;
        int64_t total_file_size_ = 0;
    };

    std::shared_ptr<Reporter> CreateReporter(const BinaryRow& partition, int32_t bucket) {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        std::pair<BinaryRow, int32_t> key(partition, bucket);
        auto reporter = std::make_shared<Reporter>(this, partition, bucket);
        reporters_[key] = reporter;
        return reporter;
    }

    void EraseReporter(const BinaryRow& partition, int32_t bucket) {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        reporters_.erase(std::pair{partition, bucket});
    }

    void ReportCompactionTime(int64_t time) {
        std::lock_guard<std::mutex> lock(compaction_times_mutex_);
        compaction_times_.push_back(time);
        if (compaction_times_.size() > kCompactionTimeWindow) {
            compaction_times_.erase(compaction_times_.begin());
        }
    }

    double MaxLevel0FileCount() {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        int64_t max_val = -1;
        for (const auto& [_, reporter] : reporters_) {
            max_val = std::max(max_val, reporter->Level0FileCount());
        }
        return static_cast<double>(max_val);
    }

    double AvgLevel0FileCount() {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        int64_t sum = 0;
        size_t n = 0;
        for (const auto& [_, reporter] : reporters_) {
            sum += reporter->Level0FileCount();
            n++;
        }
        return n > 0 ? static_cast<double>(sum) / n : -1;
    }

    double MaxCompactionInputSize() {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        int64_t max_val = -1;
        for (const auto& [_, reporter] : reporters_) {
            max_val = std::max(max_val, reporter->CompactionInputSize());
        }
        return static_cast<double>(max_val);
    }

    double MaxCompactionOutputSize() {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        int64_t max_val = -1;
        for (const auto& [_, reporter] : reporters_) {
            max_val = std::max(max_val, reporter->CompactionOutputSize());
        }
        return static_cast<double>(max_val);
    }

    double AvgCompactionInputSize() {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        int64_t sum = 0;
        size_t n = 0;
        for (const auto& [_, reporter] : reporters_) {
            sum += reporter->CompactionInputSize();
            n++;
        }
        return n > 0 ? static_cast<double>(sum) / n : -1;
    }

    double AvgCompactionOutputSize() {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        int64_t sum = 0;
        size_t n = 0;
        for (const auto& [_, reporter] : reporters_) {
            sum += reporter->CompactionOutputSize();
            n++;
        }
        return n > 0 ? static_cast<double>(sum) / n : -1;
    }

    double AvgCompactionTime() {
        std::lock_guard<std::mutex> lock(compaction_times_mutex_);
        if (compaction_times_.empty()) {
            return 0.0;
        }
        int64_t sum = 0;
        for (const auto& t : compaction_times_) {
            sum += t;
        }
        return static_cast<double>(sum) / compaction_times_.size();
    }

    double MaxTotalFileSize() {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        int64_t max_val = -1;
        for (const auto& [_, reporter] : reporters_) {
            max_val = std::max(max_val, reporter->TotalFileSize());
        }
        return static_cast<double>(max_val);
    }

    double AvgTotalFileSize() {
        std::lock_guard<std::mutex> lock(reporter_mutex_);
        int64_t sum = 0;
        size_t n = 0;
        for (const auto& [_, reporter] : reporters_) {
            sum += reporter->TotalFileSize();
            n++;
        }
        return n > 0 ? static_cast<double>(sum) / n : -1;
    }

    void IncreaseCompactionsCompletedCount() {
        compactions_completed_count_++;
    }

    void IncreaseCompactionsQueuedCount() {
        compactions_queued_count_++;
    }

    void DecreaseCompactionsQueuedCount() {
        compactions_queued_count_--;
    }

    void IncreaseCompactionsTotalCount() {
        compactions_total_count_++;
    }

    int64_t GetCompactionsCompletedCount() const {
        return compactions_completed_count_;
    }

    int64_t GetCompactionsTotalCount() const {
        return compactions_total_count_;
    }

    int64_t GetCompactionsQueuedCount() const {
        return compactions_queued_count_;
    }

    std::shared_ptr<MetricsImpl> GetMetrics() {
        auto metrics = std::make_shared<MetricsImpl>();
        metrics->SetCounter(COMPACTION_COMPLETED_COUNT, GetCompactionsCompletedCount());
        metrics->SetCounter(COMPACTION_TOTAL_COUNT, GetCompactionsTotalCount());
        metrics->SetCounter(COMPACTION_QUEUED_COUNT, GetCompactionsQueuedCount());
        metrics->SetGauge(MAX_LEVEL0_FILE_COUNT, MaxLevel0FileCount());
        metrics->SetGauge(AVG_LEVEL0_FILE_COUNT, AvgLevel0FileCount());
        metrics->SetGauge(AVG_COMPACTION_TIME, AvgCompactionTime());
        metrics->SetGauge(MAX_COMPACTION_INPUT_SIZE, MaxCompactionInputSize());
        metrics->SetGauge(MAX_COMPACTION_OUTPUT_SIZE, MaxCompactionOutputSize());
        metrics->SetGauge(AVG_COMPACTION_INPUT_SIZE, AvgCompactionInputSize());
        metrics->SetGauge(AVG_COMPACTION_OUTPUT_SIZE, AvgCompactionOutputSize());
        metrics->SetGauge(MAX_TOTAL_FILE_SIZE, MaxTotalFileSize());
        metrics->SetGauge(AVG_TOTAL_FILE_SIZE, AvgTotalFileSize());
        return metrics;
    }

 private:
    std::unordered_map<std::pair<BinaryRow, int32_t>, std::shared_ptr<Reporter>> reporters_;
    std::mutex reporter_mutex_;

    std::vector<int64_t> compaction_times_;
    std::mutex compaction_times_mutex_;

    std::atomic<int64_t> compactions_completed_count_ = {0};
    std::atomic<int64_t> compactions_total_count_ = {0};
    std::atomic<int64_t> compactions_queued_count_ = {0};
};

}  // namespace paimon
