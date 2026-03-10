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
#include <mutex>
#include <string>

#include "paimon/metrics.h"
#include "paimon/visibility.h"

namespace paimon {

class Histogram;

class PAIMON_EXPORT MetricsImpl : public Metrics {
 public:
    ~MetricsImpl() override = default;

    void SetCounter(const std::string& metric_name, uint64_t metric_value) override;
    Result<uint64_t> GetCounter(const std::string& metric_name) const override;
    std::map<std::string, uint64_t> GetAllCounters() const override;

    void ObserveHistogram(const std::string& metric_name, double value) override;
    Result<HistogramStats> GetHistogramStats(const std::string& metric_name) const override;
    std::map<std::string, HistogramStats> GetAllHistogramStats() const override;

    void SetGauge(const std::string& metric_name, double metric_value) override;
    Result<double> GetGauge(const std::string& metric_name) const override;
    std::map<std::string, double> GetAllGauges() const override;

    void Merge(const std::shared_ptr<Metrics>& other) override;
    std::string ToString() const override;
    void Overwrite(const std::shared_ptr<Metrics>& metrics);

    template <typename T>
    static std::shared_ptr<Metrics> CollectReadMetrics(const T& readers) {
        auto res_metrics = std::make_shared<MetricsImpl>();
        for (const auto& reader : readers) {
            if (!reader) {
                continue;
            }
            auto metrics = reader->GetReaderMetrics();
            if (metrics) {
                res_metrics->Merge(metrics);
            }
        }
        return res_metrics;
    }

 private:
    mutable std::mutex counter_lock_;
    std::map<std::string, uint64_t> counters_;

    mutable std::mutex histogram_lock_;
    std::map<std::string, std::shared_ptr<Histogram>> histograms_;

    mutable std::mutex gauge_lock_;
    std::map<std::string, double> gauges_;
};

}  // namespace paimon
