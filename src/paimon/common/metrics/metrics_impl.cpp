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

#include "paimon/common/metrics/metrics_impl.h"

#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/metrics/histogram_windowing.h"
#include "paimon/result.h"
#include "rapidjson/document.h"
#include "rapidjson/encodings.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {
void MetricsImpl::SetCounter(const std::string& metric_name, uint64_t metric_value) {
    std::lock_guard<std::mutex> guard(counter_lock_);
    counters_[metric_name] = metric_value;
}

Result<uint64_t> MetricsImpl::GetCounter(const std::string& metric_name) const {
    std::lock_guard<std::mutex> guard(counter_lock_);
    auto iter = counters_.find(metric_name);
    if (iter != counters_.end()) {
        return iter->second;
    }
    return Status::KeyError(fmt::format("metric '{}' not found", metric_name));
}

std::map<std::string, uint64_t> MetricsImpl::GetAllCounters() const {
    std::lock_guard<std::mutex> guard(counter_lock_);
    return counters_;
}

void MetricsImpl::ObserveHistogram(const std::string& metric_name, double value) {
    std::shared_ptr<Histogram> histogram;
    {
        std::lock_guard<std::mutex> guard(histogram_lock_);
        auto iter = histograms_.find(metric_name);
        if (iter == histograms_.end()) {
            histogram = std::make_shared<HistogramWindowingImpl>();
            histograms_.emplace(metric_name, histogram);
        } else {
            histogram = iter->second;
        }
    }
    histogram->Add(value);
}

Result<HistogramStats> MetricsImpl::GetHistogramStats(const std::string& metric_name) const {
    std::shared_ptr<Histogram> histogram;
    {
        std::lock_guard<std::mutex> guard(histogram_lock_);
        auto iter = histograms_.find(metric_name);
        if (iter == histograms_.end()) {
            return Status::KeyError(fmt::format("histogram '{}' not found", metric_name));
        }
        histogram = iter->second;
    }
    return histogram->GetStats();
}

std::map<std::string, HistogramStats> MetricsImpl::GetAllHistogramStats() const {
    std::vector<std::pair<std::string, std::shared_ptr<Histogram>>> histograms;
    {
        std::lock_guard<std::mutex> guard(histogram_lock_);
        histograms.reserve(histograms_.size());
        for (const auto& kv : histograms_) {
            histograms.push_back(kv);
        }
    }
    std::map<std::string, HistogramStats> res;
    for (const auto& [name, histogram] : histograms) {
        res.emplace(name, histogram->GetStats());
    }
    return res;
}

void MetricsImpl::SetGauge(const std::string& metric_name, double value) {
    std::lock_guard<std::mutex> lock(gauge_lock_);
    gauges_[metric_name] = value;
}

Result<double> MetricsImpl::GetGauge(const std::string& metric_name) const {
    std::lock_guard<std::mutex> lock(gauge_lock_);
    auto it = gauges_.find(metric_name);
    if (it != gauges_.end()) {
        return it->second;
    }
    return Status::KeyError(fmt::format("metric '{}' not found", metric_name));
}

std::map<std::string, double> MetricsImpl::GetAllGauges() const {
    std::lock_guard<std::mutex> lock(gauge_lock_);
    return gauges_;
}

void MetricsImpl::Merge(const std::shared_ptr<Metrics>& other) {
    if (other && this != other.get()) {
        std::map<std::string, uint64_t> other_counters = other->GetAllCounters();
        {
            std::lock_guard<std::mutex> guard(counter_lock_);
            for (const auto& kv : other_counters) {
                auto iter = counters_.find(kv.first);
                if (iter == counters_.end()) {
                    counters_[kv.first] = kv.second;
                } else {
                    counters_[kv.first] += kv.second;
                }
            }
        }
        std::map<std::string, double> other_gauges = other->GetAllGauges();
        {
            std::lock_guard<std::mutex> guard(gauge_lock_);
            for (const auto& kv : other_gauges) {
                auto iter = gauges_.find(kv.first);
                if (iter == gauges_.end()) {
                    gauges_[kv.first] = kv.second;
                } else {
                    gauges_[kv.first] += kv.second;
                }
            }
        }
        auto other_impl = std::dynamic_pointer_cast<MetricsImpl>(other);
        if (other_impl) {
            std::vector<std::pair<std::string, std::shared_ptr<Histogram>>> other_histograms;
            {
                std::lock_guard<std::mutex> guard(other_impl->histogram_lock_);
                other_histograms.reserve(other_impl->histograms_.size());
                for (const auto& kv : other_impl->histograms_) {
                    other_histograms.push_back(kv);
                }
            }

            for (const auto& [name, other_hist] : other_histograms) {
                std::shared_ptr<Histogram> this_hist;
                {
                    std::lock_guard<std::mutex> guard(histogram_lock_);
                    auto iter = histograms_.find(name);
                    if (iter == histograms_.end()) {
                        this_hist = std::make_shared<HistogramWindowingImpl>();
                        histograms_.emplace(name, this_hist);
                    } else {
                        this_hist = iter->second;
                    }
                }
                this_hist->Merge(*other_hist);
            }
        }
    }
}

void MetricsImpl::Overwrite(const std::shared_ptr<Metrics>& other) {
    if (other && this != other.get()) {
        std::map<std::string, uint64_t> other_counters = other->GetAllCounters();
        {
            std::lock_guard<std::mutex> guard(counter_lock_);
            counters_.swap(other_counters);
        }
        std::map<std::string, double> other_gauges = other->GetAllGauges();
        {
            std::lock_guard<std::mutex> guard(gauge_lock_);
            gauges_.swap(other_gauges);
        }

        auto other_impl = std::dynamic_pointer_cast<MetricsImpl>(other);
        std::map<std::string, std::shared_ptr<Histogram>> new_histograms;
        if (other_impl) {
            std::vector<std::pair<std::string, std::shared_ptr<Histogram>>> other_histograms;
            {
                std::lock_guard<std::mutex> lock(other_impl->histogram_lock_);
                other_histograms.reserve(other_impl->histograms_.size());
                for (const auto& kv : other_impl->histograms_) {
                    other_histograms.push_back(kv);
                }
            }
            for (const auto& [name, histogram] : other_histograms) {
                new_histograms.emplace(name, histogram->Clone());
            }
        }
        {
            std::lock_guard<std::mutex> lock(histogram_lock_);
            histograms_.swap(new_histograms);
        }
    }
}

std::string MetricsImpl::ToString() const {
    using RapidWriter =
        rapidjson::Writer<rapidjson::StringBuffer, rapidjson::UTF8<>, rapidjson::ASCII<>>;
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    std::map<std::string, uint64_t> counters = GetAllCounters();
    for (const auto& kv : counters) {
        doc.AddMember(rapidjson::Value(kv.first, allocator), rapidjson::Value(kv.second),
                      allocator);
    }

    // Flatten histogram stats into numeric key/value pairs to keep JSON schema simple.
    // Example keys: "read.latency.p99", "read.latency.avg", "read.latency.count".
    std::map<std::string, HistogramStats> hist_stats = GetAllHistogramStats();
    for (const auto& [name, s] : hist_stats) {
        doc.AddMember(rapidjson::Value(name + ".count", allocator), rapidjson::Value(s.count),
                      allocator);
        rapidjson::Value sum_val;
        sum_val.SetDouble(s.sum);
        doc.AddMember(rapidjson::Value(name + ".sum", allocator), sum_val, allocator);

        rapidjson::Value min_val;
        min_val.SetDouble(s.min);
        doc.AddMember(rapidjson::Value(name + ".min", allocator), min_val, allocator);

        rapidjson::Value max_val;
        max_val.SetDouble(s.max);
        doc.AddMember(rapidjson::Value(name + ".max", allocator), max_val, allocator);

        rapidjson::Value avg_val;
        avg_val.SetDouble(s.average);
        doc.AddMember(rapidjson::Value(name + ".avg", allocator), avg_val, allocator);

        rapidjson::Value p50_val;
        p50_val.SetDouble(s.p50);
        doc.AddMember(rapidjson::Value(name + ".p50", allocator), p50_val, allocator);

        rapidjson::Value p90_val;
        p90_val.SetDouble(s.p90);
        doc.AddMember(rapidjson::Value(name + ".p90", allocator), p90_val, allocator);

        rapidjson::Value p95_val;
        p95_val.SetDouble(s.p95);
        doc.AddMember(rapidjson::Value(name + ".p95", allocator), p95_val, allocator);

        rapidjson::Value p99_val;
        p99_val.SetDouble(s.p99);
        doc.AddMember(rapidjson::Value(name + ".p99", allocator), p99_val, allocator);

        rapidjson::Value p999_val;
        p999_val.SetDouble(s.p999);
        doc.AddMember(rapidjson::Value(name + ".p99.9", allocator), p999_val, allocator);

        rapidjson::Value stddev_val;
        stddev_val.SetDouble(s.stddev);
        doc.AddMember(rapidjson::Value(name + ".stddev", allocator), stddev_val, allocator);
    }

    std::map<std::string, double> gauges = GetAllGauges();
    for (const auto& kv : gauges) {
        doc.AddMember(rapidjson::Value(kv.first, allocator), rapidjson::Value(kv.second),
                      allocator);
    }

    rapidjson::StringBuffer s;
    RapidWriter writer(s);
    doc.Accept(writer);
    return s.GetString();
}

}  // namespace paimon
