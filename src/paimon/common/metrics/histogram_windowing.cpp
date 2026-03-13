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

#include "paimon/common/metrics/histogram_windowing.h"

#include <chrono>

namespace paimon {

uint64_t HistogramWindowingImpl::NowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

uint64_t HistogramWindowingImpl::AlignWindowStartMicros(uint64_t micros) const {
    if (micros_per_window_ == 0) {
        return micros;
    }
    return micros - (micros % micros_per_window_);
}

void HistogramWindowingImpl::Init() {
    std::lock_guard<std::mutex> guard(mu_);
    window_start_micros_.assign(num_windows_, std::nullopt);
    histograms_.assign(num_windows_, nullptr);
    const uint64_t now = AlignWindowStartMicros(NowMicros());
    ResetLocked(now);
}

void HistogramWindowingImpl::ResetLocked(uint64_t aligned_now) {
    if (num_windows_ == 0) {
        current_index_ = 0;
        current_window_start_micros_ = aligned_now;
        current_window_num_ = 0;
        return;
    }
    current_index_ = 0;
    current_window_start_micros_ = aligned_now;
    current_window_num_ = 0;
    for (size_t i = 0; i < num_windows_; ++i) {
        window_start_micros_[i] = std::nullopt;
        histograms_[i].reset();
    }
    window_start_micros_[current_index_] = current_window_start_micros_;
    histograms_[current_index_] = std::make_shared<HistogramImpl>();
}

void HistogramWindowingImpl::AdvanceLocked(uint64_t aligned_now) {
    if (micros_per_window_ == 0 || num_windows_ == 0) {
        return;
    }

    if (aligned_now <= current_window_start_micros_) {
        return;
    }

    const uint64_t max_span = micros_per_window_ * num_windows_;
    if (aligned_now - current_window_start_micros_ >= max_span) {
        ResetLocked(aligned_now);
        return;
    }

    // Advance at most num_windows_ steps, but only when current window has enough samples.
    while (aligned_now > current_window_start_micros_ &&
           (aligned_now - current_window_start_micros_) >= micros_per_window_ &&
           current_window_num_ >= min_num_per_window_) {
        current_window_start_micros_ += micros_per_window_;
        current_index_ = (current_index_ + 1) % static_cast<size_t>(num_windows_);
        current_window_num_ = 0;
        window_start_micros_[current_index_] = current_window_start_micros_;
        histograms_[current_index_] = std::make_shared<HistogramImpl>();
    }
}

size_t HistogramWindowingImpl::IndexForStartLocked(uint64_t aligned_start) const {
    // Find the slot with the same start time.
    for (size_t i = 0; i < window_start_micros_.size(); ++i) {
        if (window_start_micros_[i].has_value() &&
            window_start_micros_[i].value() == aligned_start) {
            return i;
        }
    }
    // Not found; map by offset from current window.
    if (micros_per_window_ == 0) {
        return current_index_;
    }
    if (aligned_start > current_window_start_micros_) {
        return current_index_;
    }
    const uint64_t delta = current_window_start_micros_ - aligned_start;
    const uint64_t steps = delta / micros_per_window_;
    const size_t idx = (current_index_ + num_windows_ - (steps % num_windows_)) %
                       static_cast<size_t>(num_windows_);
    return idx;
}

std::shared_ptr<HistogramImpl> HistogramWindowingImpl::GetOrCreateForStartLocked(
    uint64_t aligned_start) {
    if (num_windows_ == 0) {
        return nullptr;
    }
    size_t idx = IndexForStartLocked(aligned_start);
    window_start_micros_[idx] = aligned_start;
    if (!histograms_[idx]) {
        histograms_[idx] = std::make_shared<HistogramImpl>();
    }
    return histograms_[idx];
}

void HistogramWindowingImpl::Add(double value) {
    if (num_windows_ == 0) {
        return;
    }
    std::shared_ptr<HistogramImpl> hist;
    {
        std::lock_guard<std::mutex> guard(mu_);
        const uint64_t now = NowMicros();
        const uint64_t aligned_now = AlignWindowStartMicros(now);
        // Ignore if clock moves backwards.
        if (aligned_now < current_window_start_micros_) {
            return;
        }
        AdvanceLocked(aligned_now);
        if (!histograms_[current_index_]) {
            histograms_[current_index_] = std::make_shared<HistogramImpl>();
            window_start_micros_[current_index_] = current_window_start_micros_;
        }
        hist = histograms_[current_index_];
        ++current_window_num_;
    }
    if (hist) {
        hist->Add(value);
    }
}

HistogramStats HistogramWindowingImpl::GetStats() const {
    if (num_windows_ == 0) {
        return HistogramStats{};
    }
    std::vector<std::pair<uint64_t, std::shared_ptr<HistogramImpl>>> hists;
    uint64_t aligned_now = 0;
    {
        std::lock_guard<std::mutex> guard(mu_);
        aligned_now = AlignWindowStartMicros(NowMicros());
        // Note: do NOT force AdvanceLocked here, to keep semantics consistent with RocksDB's
        // min_num_per_window behavior (windows advance upon Add() after enough samples).
        hists.reserve(histograms_.size());
        for (size_t i = 0; i < histograms_.size(); ++i) {
            if (!window_start_micros_[i].has_value() || !histograms_[i]) {
                continue;
            }
            const uint64_t start = window_start_micros_[i].value();
            hists.emplace_back(start, histograms_[i]);
        }
    }

    HistogramImpl aggregated;
    if (micros_per_window_ == 0 || num_windows_ == 0) {
        for (const auto& kv : hists) {
            aggregated.MergeFrom(*kv.second);
        }
        return aggregated.GetStats();
    }

    const uint64_t max_span = micros_per_window_ * num_windows_;
    for (const auto& kv : hists) {
        const auto start = kv.first;
        const auto& h = kv.second;
        if (aligned_now >= start && (aligned_now - start) < max_span) {
            aggregated.MergeFrom(*h);
        }
    }
    return aggregated.GetStats();
}

void HistogramWindowingImpl::Merge(const Histogram& other) {
    const auto* other_w = dynamic_cast<const HistogramWindowingImpl*>(&other);
    if (!other_w) {
        const auto* other_impl = dynamic_cast<const HistogramImpl*>(&other);
        if (!other_impl) {
            return;
        }
        // Merge a plain histogram into current window.
        std::shared_ptr<HistogramImpl> target;
        {
            std::lock_guard<std::mutex> guard(mu_);
            const uint64_t aligned_now = AlignWindowStartMicros(NowMicros());
            if (aligned_now < current_window_start_micros_) {
                return;
            }
            AdvanceLocked(aligned_now);
            target = GetOrCreateForStartLocked(current_window_start_micros_);
        }
        if (target) {
            target->MergeFrom(*other_impl);
        }
        return;
    }

    // If windowing parameters mismatch, fall back to merging all other windows into current.
    if (num_windows_ != other_w->num_windows_ ||
        micros_per_window_ != other_w->micros_per_window_ ||
        min_num_per_window_ != other_w->min_num_per_window_) {
        std::vector<std::shared_ptr<HistogramImpl>> other_only;
        {
            std::lock_guard<std::mutex> guard(other_w->mu_);
            other_only.reserve(other_w->histograms_.size());
            for (const auto& histogram : other_w->histograms_) {
                if (histogram) {
                    other_only.push_back(histogram);
                }
            }
        }
        std::shared_ptr<HistogramImpl> target;
        {
            std::lock_guard<std::mutex> guard(mu_);
            const uint64_t aligned_now = AlignWindowStartMicros(NowMicros());
            if (aligned_now < current_window_start_micros_) {
                return;
            }
            AdvanceLocked(aligned_now);
            target = GetOrCreateForStartLocked(current_window_start_micros_);
        }
        if (target) {
            for (const auto& h : other_only) {
                if (h) {
                    target->MergeFrom(*h);
                }
            }
        }
        return;
    }

    // Merge by window start timestamps.
    std::vector<std::pair<uint64_t, std::shared_ptr<HistogramImpl>>> other_hists;
    {
        std::lock_guard<std::mutex> guard(other_w->mu_);
        other_hists.reserve(other_w->histograms_.size());
        for (size_t i = 0; i < other_w->histograms_.size(); ++i) {
            if (!other_w->window_start_micros_[i].has_value() || !other_w->histograms_[i]) {
                continue;
            }
            other_hists.emplace_back(other_w->window_start_micros_[i].value(),
                                     other_w->histograms_[i]);
        }
    }

    {
        std::lock_guard<std::mutex> guard(mu_);
        const uint64_t aligned_now = AlignWindowStartMicros(NowMicros());
        if (aligned_now < current_window_start_micros_) {
            return;
        }
        AdvanceLocked(aligned_now);
        const uint64_t max_span = micros_per_window_ * num_windows_;
        for (const auto& kv : other_hists) {
            const auto start = kv.first;
            const auto& hist = kv.second;
            if (aligned_now >= start && (aligned_now - start) < max_span) {
                auto target = GetOrCreateForStartLocked(start);
                if (target && hist) {
                    target->MergeFrom(*hist);
                }
            }
        }
    }
}

std::shared_ptr<Histogram> HistogramWindowingImpl::Clone() const {
    auto cloned = std::make_shared<HistogramWindowingImpl>(num_windows_, micros_per_window_,
                                                           min_num_per_window_);
    std::lock_guard<std::mutex> guard(mu_);
    std::lock_guard<std::mutex> guard2(cloned->mu_);

    cloned->current_window_start_micros_ = current_window_start_micros_;
    cloned->current_window_num_ = current_window_num_;
    cloned->current_index_ = current_index_;
    cloned->window_start_micros_ = window_start_micros_;
    cloned->histograms_.assign(num_windows_, nullptr);
    for (size_t i = 0; i < histograms_.size() && i < cloned->histograms_.size(); ++i) {
        const auto& histogram = histograms_[i];
        if (histogram) {
            cloned->histograms_[i] = std::dynamic_pointer_cast<HistogramImpl>(histogram->Clone());
        }
    }
    return cloned;
}

}  // namespace paimon
