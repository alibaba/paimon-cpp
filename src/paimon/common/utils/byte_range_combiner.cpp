/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Adapted from Apache ORC
// https://github.com/apache/orc/blob/main/c%2B%2B/src/io/Cache.cc

#include "paimon/common/utils/byte_range_combiner.h"

#include <algorithm>
#include <cassert>

#include "fmt/format.h"

namespace paimon {

Result<std::vector<ByteRange>> ByteRangeCombiner::Coalesce(std::vector<ByteRange>&& ranges) const {
    if (ranges.empty()) {
        return ranges;
    }
    // Remove zero-sized ranges
    auto end = std::remove_if(ranges.begin(), ranges.end(),
                              [](const ByteRange& range) { return range.length == 0; });
    // Sort in position order
    std::sort(ranges.begin(), end, [](const ByteRange& a, const ByteRange& b) {
        // Prefer longer ranges at same offset to simplify deduplication
        return a.offset != b.offset ? a.offset < b.offset : a.length > b.length;
    });

    // Remove ranges that overlap 100%
    std::vector<ByteRange> unique_ranges;
    unique_ranges.reserve(ranges.size());
    for (auto it = ranges.begin(); it != end; ++it) {
        if (unique_ranges.empty() || !unique_ranges.back().Contains(*it)) {
            unique_ranges.emplace_back(*it);
        }
    }
    ranges = std::move(unique_ranges);

    // Skip further processing if ranges is empty after removing zero-sized ranges.
    if (ranges.empty()) {
        return ranges;
    }

    for (size_t i = 0; i < ranges.size() - 1; ++i) {
        const auto& left = ranges[i];
        const auto& right = ranges[i + 1];
        if (left.offset >= right.offset || left.Contains(right)) {
            return Status::Invalid("Byte ranges must be non-overlapping and sorted.");
        }
    }

    std::vector<ByteRange> coalesced;
    auto iter = ranges.begin();

    // Start of the current coalesced range and end (exclusive) of previous range.
    // Both are initialized with the start of first range which is a placeholder value.
    uint64_t coalesced_start = iter->offset;
    uint64_t coalesced_end = coalesced_start + iter->length;

    for (++iter; iter < ranges.end(); ++iter) {
        const uint64_t current_range_start = iter->offset;
        const uint64_t current_range_end = current_range_start + iter->length;

        assert(coalesced_start < coalesced_end);
        assert(current_range_start < current_range_end);

        // At this point, the coalesced range is [coalesced_start, prev_range_end).
        // Stop coalescing if:
        //   - coalesced range is too large, or
        //   - distance (hole/gap) between consecutive ranges is too large.
        if ((current_range_end - coalesced_start > range_size_limit) ||
            (current_range_start > coalesced_end + hole_size_limit)) {
            coalesced.emplace_back(coalesced_start, coalesced_end - coalesced_start);
            coalesced_start = current_range_start;
        }

        // Update the prev_range_end with the current range.
        coalesced_end = current_range_end;
    }
    coalesced.emplace_back(coalesced_start, coalesced_end - coalesced_start);
    assert(coalesced.front().offset == ranges.front().offset);
    assert(coalesced.back().offset + coalesced.back().length ==
           ranges.back().offset + ranges.back().length);
    return coalesced;
}

Result<std::vector<ByteRange>> ByteRangeCombiner::CoalesceByteRanges(
    std::vector<ByteRange>&& ranges, uint64_t hole_size_limit, uint64_t range_size_limit) {
    if (range_size_limit <= hole_size_limit) {
        return Status::Invalid(
            fmt::format("range size limit {} should be larger than hole size limit {}",
                        range_size_limit, hole_size_limit));
    }
    ByteRangeCombiner combiner{hole_size_limit, range_size_limit};
    return combiner.Coalesce(std::move(ranges));
}

}  // namespace paimon
