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

#include "paimon/format/orc/read_range_generator.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <exception>
#include <limits>
#include <unordered_set>

#include "fmt/format.h"
#include "orc/Common.hh"
#include "orc/Reader.hh"
#include "orc/orc-config.hh"
#include "paimon/common/utils/options_utils.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/status.h"

namespace paimon::orc {

Result<std::unique_ptr<ReadRangeGenerator>> ReadRangeGenerator::Create(
    const ::orc::Reader* reader, uint64_t natural_read_size,
    const std::map<std::string, std::string>& options) {
    try {
        if (reader == nullptr) {
            return Status::Invalid("create read range generator failed, orc reader is nullptr.");
        }
        std::map<uint64_t, std::map<::orc::StreamKind, uint64_t>> column_length_map;
        uint64_t stripe_num = reader->getNumberOfStripes();
        std::vector<std::unique_ptr<::orc::StripeInformation>> stripe_infos;
        stripe_infos.reserve(stripe_num);
        for (uint64_t i = 0; i < stripe_num; i++) {
            stripe_infos.emplace_back(reader->getStripe(i));
        }
        if (stripe_num > 0) {
            const auto& stripe = stripe_infos[0].get();
            for (uint64_t s = 0; s < stripe->getNumberOfStreams(); ++s) {
                std::unique_ptr<::orc::StreamInformation> stream = stripe->getStreamInformation(s);
                uint64_t column_id = stream->getColumnId();
                uint64_t length = stream->getLength();
                column_length_map[column_id][stream->getKind()] = length;
            }
        }
        return std::unique_ptr<ReadRangeGenerator>(new ReadRangeGenerator(
            reader, natural_read_size, std::move(stripe_infos), column_length_map, options));
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("create read range generator failed, with {} error", e.what()));
    } catch (...) {
        return Status::UnknownError("create read range generator failed, with unknown error");
    }
}

Result<std::vector<std::pair<uint64_t, uint64_t>>> ReadRangeGenerator::GenReadRanges(
    const std::vector<uint64_t>& target_column_ids, uint64_t begin_row_num, uint64_t end_row_num,
    bool* need_prefetch) const {
    try {
        *need_prefetch = false;
        uint64_t stripe_num = reader_->getNumberOfStripes();
        if (stripe_num == 0) {
            return std::vector<std::pair<uint64_t, uint64_t>>();
        }
        ReadRangeGenerator::ReaderMeta reader_meta = GetReaderMeta();
        uint32_t suggest_row_count = static_cast<uint32_t>(
            std::min(SuggestRowCount(target_column_ids),
                     static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())));
        auto ranges = DoGenReadRanges(begin_row_num, end_row_num, suggest_row_count, reader_meta);

        uint64_t target_read_length = 0;
        for (const auto& column_id : target_column_ids) {
            target_read_length += CompressedLength(column_id, std::nullopt);
        }
        target_read_length *= reader_->getNumberOfStripes();
        PAIMON_ASSIGN_OR_RAISE(
            uint64_t enable_prefetch_read_size_threshold,
            OptionsUtils::GetValueFromMap<uint64_t>(options_, ENABLE_PREFETCH_READ_SIZE_THRESHOLD,
                                                    DEFAULT_ENABLE_PREFETCH_READ_SIZE_THRESHOLD));
        if (target_read_length > enable_prefetch_read_size_threshold) {
            *need_prefetch = true;
        }

        return ranges;
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("gen read ranges failed, with {} error", e.what()));
    } catch (...) {
        return Status::UnknownError("gen read ranges failed, with unknown error");
    }
}

std::vector<std::pair<uint64_t, uint64_t>> ReadRangeGenerator::DoGenReadRanges(
    uint64_t begin_row_num, uint64_t end_row_num, uint32_t range_size,
    const ReaderMeta& reader_meta) {
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges;
    auto calculate_range_size = [range_size](uint64_t pos, uint64_t stripe_end) -> uint64_t {
        uint64_t min_right_bound = std::min({pos + range_size, stripe_end});
        return min_right_bound - pos;
    };

    uint64_t current_row = begin_row_num;
    uint64_t next_range_size = range_size;
    while (current_row < end_row_num) {
        uint64_t curr_stripe_begin = 0;
        for (auto stripe_row_count : reader_meta.rows_per_stripes) {
            if (current_row >= curr_stripe_begin &&
                current_row < curr_stripe_begin + stripe_row_count) {
                next_range_size =
                    calculate_range_size(current_row - curr_stripe_begin, stripe_row_count);
                break;
            } else {
                curr_stripe_begin += stripe_row_count;
            }
        }
        read_ranges.emplace_back(current_row, std::min(current_row + next_range_size, end_row_num));
        current_row += next_range_size;
    }
    return read_ranges;
}

uint64_t ReadRangeGenerator::SuggestRowCount(const std::vector<uint64_t>& target_column_ids) const {
    double bytes_per_group = BytesPerGroup(target_column_ids);
    uint64_t expect_row_group_count =
        std::ceil(static_cast<double>(natural_read_size_) / bytes_per_group);
    expect_row_group_count =
        std::max(expect_row_group_count, MIN_ROW_GROUP_COUNT_IN_ONE_NATURAL_READ);
    return std::min(expect_row_group_count * reader_->getRowIndexStride(), MaxRowCountInStripe());
}

double ReadRangeGenerator::BytesPerGroup(const std::vector<uint64_t>& column_ids) const {
    if (reader_->getNumberOfRows() == 0) {
        return 1;
    }
    uint64_t max_column_id = std::numeric_limits<uint64_t>::max();
    uint64_t max_length = 0;
    for (const auto& column_id : column_ids) {
        uint64_t length = CompressedLength(column_id, std::nullopt);
        if (length > max_length) {
            max_length = length;
            max_column_id = column_id;
        }
    }
    if (max_column_id == std::numeric_limits<uint64_t>::max()) {
        return 1;
    }
    uint64_t stripe_row_count = stripe_infos_[0]->getNumberOfRows();
    if (stripe_row_count == 0) {
        return 1;
    }
    double avg_len_per_row = static_cast<double>(max_length) / stripe_row_count;
    double bytes_per_group = avg_len_per_row * reader_->getRowIndexStride();
    if (bytes_per_group < 1) {
        return 1;
    }
    return bytes_per_group;
}

ReadRangeGenerator::ReaderMeta ReadRangeGenerator::GetReaderMeta() const {
    ReaderMeta reader_meta;
    reader_meta.rows_per_group = reader_->getRowIndexStride();
    uint64_t stripe_num = reader_->getNumberOfStripes();
    for (uint64_t i = 0; i < stripe_num; i++) {
        auto stripe_row_num = stripe_infos_[i]->getNumberOfRows();
        reader_meta.rows_per_stripes.push_back(stripe_row_num);
    }
    return reader_meta;
}

uint64_t ReadRangeGenerator::MaxRowCountInStripe() const {
    uint64_t max_row_num_per_stripe = 0;
    uint64_t stripe_num = reader_->getNumberOfStripes();
    if (stripe_num == 0) {
        return 0;
    }
    for (uint64_t i = 0; i < stripe_num; i++) {
        auto stripe_row_num = stripe_infos_[i]->getNumberOfRows();
        max_row_num_per_stripe = std::max(max_row_num_per_stripe, stripe_row_num);
    }
    return max_row_num_per_stripe;
}

uint64_t ReadRangeGenerator::CompressedLength(uint32_t col_id,
                                              std::optional<::orc::StreamKind> stream_kind) const {
    uint64_t length = 0;
    auto iter = column_length_map_.find(col_id);
    if (iter != column_length_map_.end()) {
        auto& kind_and_length = iter->second;
        if (stream_kind) {
            auto it = kind_and_length.find(stream_kind.value());
            if (it != kind_and_length.end()) {
                length = it->second;
            } else {
                assert(false);
            }
        } else {
            for (const auto& [_, len] : kind_and_length) {
                length += len;
            }
        }
    } else {
        assert(false);
    }
    return length;
}

ReadRangeGenerator::ReadRangeGenerator(
    const ::orc::Reader* reader, uint64_t natural_read_size,
    std::vector<std::unique_ptr<::orc::StripeInformation>>&& stripe_infos,
    const std::map<uint64_t, std::map<::orc::StreamKind, uint64_t>>& column_length_map,
    const std::map<std::string, std::string>& options)
    : reader_(reader),
      natural_read_size_(natural_read_size),
      stripe_infos_(std::move(stripe_infos)),
      column_length_map_(column_length_map),
      options_(options) {}

}  // namespace paimon::orc
