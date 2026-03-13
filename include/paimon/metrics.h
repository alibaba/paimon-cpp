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

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "paimon/type_fwd.h"

namespace paimon {

/// Statistics snapshot for a histogram metric.
///
/// Note: percentile values are estimated from internal buckets.
struct PAIMON_EXPORT HistogramStats {
    uint64_t count = 0;
    double sum = 0;
    double min = 0;
    double max = 0;
    double average = 0;
    double p50 = 0;
    double p90 = 0;
    double p95 = 0;
    double p99 = 0;
    double p999 = 0;
    double stddev = 0;
};

/// Abstract interface for collecting and managing performance metrics in Paimon operations.
///
/// This class provides a unified interface for tracking various performance metrics
/// such as counters for read/write operations, I/O statistics, and other operational
/// measurements. It serves as the base class for concrete implementations like `MetricsImpl`.
class PAIMON_EXPORT Metrics {
 public:
    virtual ~Metrics() = default;

    /// Set the value of a specific counter metric.
    /// @param metric_name The name/key of the metric to set.
    /// @param metric_value The value to set for this metric.
    virtual void SetCounter(const std::string& metric_name, uint64_t metric_value) = 0;

    /// Get the current value of a specific counter metric.
    /// @param metric_name The name/key of the metric to retrieve.
    /// @return The current value of the metric, or `Status::KeyError` if the metric doesn't exist.
    virtual Result<uint64_t> GetCounter(const std::string& metric_name) const = 0;

    /// Get all counter metrics as a map.
    /// @return A map containing all metric names and their current values.
    virtual std::map<std::string, uint64_t> GetAllCounters() const = 0;

    /// Add a sample to a histogram metric.
    /// @param metric_name The name/key of the histogram metric.
    /// @param value The observed value.
    virtual void ObserveHistogram(const std::string& metric_name, double value) = 0;

    /// Get histogram statistics snapshot.
    /// @return `Status::KeyError` if the histogram doesn't exist.
    virtual Result<HistogramStats> GetHistogramStats(const std::string& metric_name) const = 0;

    /// Get all histogram statistics snapshots.
    virtual std::map<std::string, HistogramStats> GetAllHistogramStats() const = 0;

    /// Set the value of a specific gauge metric (current state metric).
    /// @param metric_name The name/key of the gauge metric to set.
    /// @param metric_value The value to set for this gauge metric (double, not just integer).
    virtual void SetGauge(const std::string& metric_name, double metric_value) = 0;

    /// Get the current value of a specific gauge metric (current state metric).
    /// @param metric_name The name/key of the gauge metric to retrieve.
    /// @return The current value of the gauge metric, or `Status::KeyError` if the metric doesn't
    /// exist.
    virtual Result<double> GetGauge(const std::string& metric_name) const = 0;

    /// Get all gauge metrics as a map.
    /// @return A map containing all gauge metric names and their current values.
    virtual std::map<std::string, double> GetAllGauges() const = 0;

    /// Merge metrics from another Metrics instance into this one.
    ///
    /// For metrics that exist in both instances, the values are added together.
    /// For metrics that only exist in the other instance, they are copied over.
    /// This operation is useful for aggregating metrics from multiple sources.
    ///
    /// @param other The other Metrics instance to merge from. If `other` is nullptr or same as
    /// this, no operation is performed.
    virtual void Merge(const std::shared_ptr<Metrics>& other) = 0;

    /// Convert all metrics to a JSON string representation.
    /// @return A JSON string containing all metric names and values, e.g.,
    /// `{"metric1":100,"metric2":200}`.
    virtual std::string ToString() const = 0;
};

}  // namespace paimon
