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
#include "paimon/core/core_options.h"
namespace paimon {
/// OffPeakHours to control compaction ratio by hours.
class OffPeakHours {
 public:
    /// @return Pointer to `OffPeakHours` if the options contain OffPeakHours settings; otherwise,
    /// nullptr.
    static std::shared_ptr<OffPeakHours> Create(const CoreOptions& options) {
        return Create(options.GetCompactOffPeakStartHour(), options.GetCompactOffPeakEndHour(),
                      options.GetCompactOffPeakRatio());
    }

    static std::shared_ptr<OffPeakHours> Create(int32_t start_hour, int32_t end_hour,
                                                int32_t compact_off_peak_ratio) {
        if (start_hour == -1 || end_hour == -1) {
            return nullptr;
        }
        if (start_hour == end_hour) {
            return nullptr;
        }
        return std::shared_ptr<OffPeakHours>(
            new OffPeakHours(start_hour, end_hour, compact_off_peak_ratio));
    }

    int32_t CurrentRatio(int32_t target_hour) const {
        bool is_off_peak;
        if (start_hour_ <= end_hour_) {
            is_off_peak = start_hour_ <= target_hour && target_hour < end_hour_;
        } else {
            is_off_peak = target_hour < end_hour_ || start_hour_ <= target_hour;
        }
        return is_off_peak ? compact_off_peak_ratio_ : 0;
    }

 private:
    OffPeakHours(int32_t start_hour, int32_t end_hour, int32_t compact_off_peak_ratio)
        : start_hour_(start_hour),
          end_hour_(end_hour),
          compact_off_peak_ratio_(compact_off_peak_ratio) {}

 private:
    int32_t start_hour_;
    int32_t end_hour_;
    int32_t compact_off_peak_ratio_;
};
}  // namespace paimon
