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
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

#include "paimon/metrics.h"

namespace paimon {

// Internal histogram interface used by MetricsImpl.
//
// Note: this interface is intentionally NOT exported.
class Histogram {
 public:
    virtual ~Histogram() = default;

    virtual void Add(double value) = 0;
    virtual HistogramStats GetStats() const = 0;
    virtual void Merge(const Histogram& other) = 0;
    virtual std::shared_ptr<Histogram> Clone() const = 0;
};

// Internal histogram implementation.
//
// The bucket boundaries are aligned with RocksDB's HistogramBucketMapper
// (rocksdb/monitoring/histogram.cc).
class HistogramImpl : public Histogram {
 public:
    HistogramImpl();

    void Add(double value) override;

    HistogramStats GetStats() const override;

    void Merge(const Histogram& other) override;

    std::shared_ptr<Histogram> Clone() const override;

    struct Snapshot {
        uint64_t count = 0;
        double sum = 0;
        double sum_squares = 0;
        double min = 0;
        double max = 0;
        std::vector<uint64_t> bucket_counts;
    };

    Snapshot GetSnapshot() const;

    void MergeFrom(const HistogramImpl& other);

    static HistogramStats ToStats(const Snapshot& s);

 private:
    static const std::vector<double>& BucketLimits();
    static size_t BucketIndex(double value, const std::vector<double>& limits);
    static double EstimatePercentile(const Snapshot& s, double p);
    static double EstimateStddev(const Snapshot& s);

 private:
    mutable std::mutex mu_;
    uint64_t num_ = 0;
    double sum_ = 0;
    double sum_squares_ = 0;
    double min_ = std::numeric_limits<double>::infinity();
    double max_ = 0;
    std::vector<uint64_t> bucket_counts_;
};

}  // namespace paimon
