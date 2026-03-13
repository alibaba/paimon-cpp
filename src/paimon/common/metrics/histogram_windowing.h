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

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "paimon/common/metrics/histogram.h"

namespace paimon {

// Window-based histogram implementation.
//
// Inspired by RocksDB's monitoring/histogram_windowing.h.
// Maintains multiple time windows and aggregates stats across the recent ones.
class HistogramWindowingImpl : public Histogram {
 public:
    // Default: 3 windows, 60s per window , 100 per window (paimon-java default)
    HistogramWindowingImpl()
        : num_windows_(3), micros_per_window_(60 * 1000ULL * 1000ULL), min_num_per_window_(100) {
        Init();
    }

    HistogramWindowingImpl(uint64_t num_windows, uint64_t micros_per_window,
                           uint64_t min_num_per_window)
        : num_windows_(num_windows),
          micros_per_window_(micros_per_window),
          min_num_per_window_(min_num_per_window) {
        Init();
    }

    void Add(double value) override;
    HistogramStats GetStats() const override;
    void Merge(const Histogram& other) override;
    std::shared_ptr<Histogram> Clone() const override;

 private:
    static uint64_t NowMicros();
    uint64_t AlignWindowStartMicros(uint64_t micros) const;

    void Init();
    void ResetLocked(uint64_t aligned_now);
    void AdvanceLocked(uint64_t aligned_now);
    size_t IndexForStartLocked(uint64_t aligned_start) const;
    std::shared_ptr<HistogramImpl> GetOrCreateForStartLocked(uint64_t aligned_start);

 private:
    const uint64_t num_windows_;
    const uint64_t micros_per_window_;
    const uint64_t min_num_per_window_;

    mutable std::mutex mu_;

    // State protected by mu_.
    uint64_t current_window_start_micros_ = 0;
    uint64_t current_window_num_ = 0;
    size_t current_index_ = 0;

    // Per slot: start timestamp and histogram.
    std::vector<std::optional<uint64_t>> window_start_micros_;
    std::vector<std::shared_ptr<HistogramImpl>> histograms_;
};

}  // namespace paimon
