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

#include "paimon/common/metrics/histogram.h"

#include <algorithm>
#include <cmath>

namespace paimon {

HistogramImpl::HistogramImpl() : bucket_counts_(BucketLimits().size(), 0) {}

void HistogramImpl::Add(double value) {
    if (std::isnan(value)) {
        return;
    }
    std::lock_guard<std::mutex> guard(mu_);
    const auto& limits = BucketLimits();
    size_t idx = BucketIndex(value, limits);
    bucket_counts_[idx] += 1;
    num_ += 1;
    sum_ += value;
    sum_squares_ += value * value;
    if (value < min_) {
        min_ = value;
    }
    if (value > max_) {
        max_ = value;
    }
}

HistogramStats HistogramImpl::GetStats() const {
    return ToStats(GetSnapshot());
}

HistogramImpl::Snapshot HistogramImpl::GetSnapshot() const {
    std::lock_guard<std::mutex> guard(mu_);
    Snapshot s;
    s.count = num_;
    s.sum = sum_;
    s.sum_squares = sum_squares_;
    s.min = (num_ == 0) ? 0 : min_;
    s.max = (num_ == 0) ? 0 : max_;
    s.bucket_counts = bucket_counts_;
    return s;
}

void HistogramImpl::Merge(const Histogram& other) {
    const auto* other_impl = dynamic_cast<const HistogramImpl*>(&other);
    if (!other_impl) {
        return;
    }
    MergeFrom(*other_impl);
}

void HistogramImpl::MergeFrom(const HistogramImpl& other) {
    Snapshot other_snapshot = other.GetSnapshot();
    std::lock_guard<std::mutex> guard(mu_);
    if (bucket_counts_.size() != other_snapshot.bucket_counts.size()) {
        // Should never happen unless bucket config changes.
        return;
    }
    if (other_snapshot.count == 0) {
        return;
    }
    for (size_t i = 0; i < bucket_counts_.size(); ++i) {
        bucket_counts_[i] += other_snapshot.bucket_counts[i];
    }
    if (num_ == 0) {
        min_ = other_snapshot.min;
        max_ = other_snapshot.max;
    } else {
        min_ = std::min(min_, other_snapshot.min);
        max_ = std::max(max_, other_snapshot.max);
    }
    num_ += other_snapshot.count;
    sum_ += other_snapshot.sum;
    sum_squares_ += other_snapshot.sum_squares;
}

std::shared_ptr<Histogram> HistogramImpl::Clone() const {
    Snapshot s = GetSnapshot();
    auto cloned = std::make_shared<HistogramImpl>();
    std::lock_guard<std::mutex> guard(cloned->mu_);
    cloned->num_ = s.count;
    cloned->sum_ = s.sum;
    cloned->sum_squares_ = s.sum_squares;
    cloned->min_ = (s.count == 0) ? std::numeric_limits<double>::infinity() : s.min;
    cloned->max_ = s.max;
    cloned->bucket_counts_ = std::move(s.bucket_counts);
    return cloned;
}

HistogramStats HistogramImpl::ToStats(const Snapshot& s) {
    HistogramStats stats;
    stats.count = s.count;
    stats.sum = s.sum;
    stats.min = s.min;
    stats.max = s.max;
    stats.average = (s.count == 0) ? 0 : (s.sum / static_cast<double>(s.count));
    stats.p50 = EstimatePercentile(s, 0.50);
    stats.p90 = EstimatePercentile(s, 0.90);
    stats.p95 = EstimatePercentile(s, 0.95);
    stats.p99 = EstimatePercentile(s, 0.99);
    stats.p999 = EstimatePercentile(s, 0.999);
    stats.stddev = EstimateStddev(s);
    return stats;
}

const std::vector<double>& HistogramImpl::BucketLimits() {
    static const std::vector<double> limits = []() {
        // Keep bucket generation aligned with RocksDB's HistogramBucketMapper
        // (see rocksdb/monitoring/histogram.cc).
        //
        // The mapper starts from {1,2}, then grows by *1.5 and rounds each bucket boundary to
        // be human-readable by keeping the two most significant digits.
        std::vector<double> v;
        v.reserve(256);
        v.push_back(1);
        v.push_back(2);

        double bucket_val = v.back();
        const auto max_u64 = static_cast<double>(std::numeric_limits<uint64_t>::max());
        while ((bucket_val = 1.5 * bucket_val) <= max_u64) {
            auto rounded = static_cast<uint64_t>(bucket_val);
            // Keep two most significant digits (e.g., 172 -> 170).
            uint64_t pow_of_ten = 1;
            while (rounded / 10 > 10) {
                rounded /= 10;
                pow_of_ten *= 10;
            }
            rounded *= pow_of_ten;
            v.push_back(static_cast<double>(rounded));
        }
        return v;
    }();
    return limits;
}

size_t HistogramImpl::BucketIndex(double value, const std::vector<double>& limits) {
    if (value <= limits.front()) {
        return 0;
    }
    auto it = std::lower_bound(limits.begin(), limits.end(), value);
    if (it == limits.end()) {
        return limits.size() - 1;
    }
    return static_cast<size_t>(std::distance(limits.begin(), it));
}

double HistogramImpl::EstimatePercentile(const Snapshot& s, double p) {
    if (s.count == 0) {
        return 0;
    }
    if (p <= 0) {
        return s.min;
    }
    if (p >= 1) {
        return s.max;
    }
    const auto& limits = BucketLimits();
    const auto rank = static_cast<uint64_t>(std::ceil(p * static_cast<double>(s.count)));
    uint64_t cum = 0;
    for (size_t i = 0; i < s.bucket_counts.size(); ++i) {
        uint64_t bcnt = s.bucket_counts[i];
        if (bcnt == 0) {
            continue;
        }
        uint64_t prev = cum;
        cum += bcnt;
        if (cum < rank) {
            continue;
        }

        // Interpolate inside bucket: estimate that samples are uniformly distributed
        // between bucket lower and upper bounds.
        double upper = (i < limits.size()) ? limits[i] : s.max;
        double lower = (i == 0) ? std::min(s.min, 0.0) : limits[i - 1];
        if (std::isinf(upper)) {
            return s.max;
        }
        const double pos_in_bucket = static_cast<double>(rank - prev) / static_cast<double>(bcnt);
        double est = lower + (upper - lower) * pos_in_bucket;
        if (est < s.min) {
            est = s.min;
        }
        if (est > s.max) {
            est = s.max;
        }
        return est;
    }
    return s.max;
}

double HistogramImpl::EstimateStddev(const Snapshot& s) {
    if (s.count == 0) {
        return 0;
    }
    const auto n = static_cast<double>(s.count);
    const double mean = s.sum / n;
    const double ex2 = s.sum_squares / n;
    const double var = std::max(0.0, ex2 - mean * mean);
    return std::sqrt(var);
}

}  // namespace paimon
