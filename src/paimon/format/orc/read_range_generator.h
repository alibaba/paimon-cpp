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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "orc/Common.hh"
#include "orc/Reader.hh"
#include "orc/orc-config.hh"
#include "paimon/common/utils/options_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/result.h"

namespace orc {
class Reader;
}  // namespace orc

namespace paimon::orc {

class ReadRangeGenerator {
 public:
    static Result<std::unique_ptr<ReadRangeGenerator>> Create(
        const ::orc::Reader* reader, uint64_t natural_read_size,
        const std::map<std::string, std::string>& options);

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        const std::vector<uint64_t>& target_column_ids, uint64_t begin_row_num,
        uint64_t end_row_num, bool* need_prefetch) const;

 private:
    struct ReaderMeta {
        uint64_t rows_per_group;
        std::vector<uint64_t> rows_per_stripes;
    };

    static std::vector<std::pair<uint64_t, uint64_t>> DoGenReadRanges(
        uint64_t begin_row_num, uint64_t end_row_num, uint32_t range_size,
        const ReaderMeta& reader_meta);

    ReadRangeGenerator(
        const ::orc::Reader* reader, uint64_t natural_read_size,
        std::vector<std::unique_ptr<::orc::StripeInformation>>&& stripe_infos,
        const std::map<uint64_t, std::map<::orc::StreamKind, uint64_t>>& column_length_map,
        const std::map<std::string, std::string>& options);

    uint64_t SuggestRowCount(const std::vector<uint64_t>& target_column_ids) const;
    double BytesPerGroup(const std::vector<uint64_t>& column_ids) const;
    ReaderMeta GetReaderMeta() const;
    uint64_t MaxRowCountInStripe() const;
    uint64_t CompressedLength(uint32_t col_id, std::optional<::orc::StreamKind> stream_kind) const;

    const ::orc::Reader* reader_;
    const uint64_t natural_read_size_;
    const std::vector<std::unique_ptr<::orc::StripeInformation>> stripe_infos_;
    const std::map<uint64_t, std::map<::orc::StreamKind, uint64_t>> column_length_map_;
    const std::map<std::string, std::string> options_;
};

}  // namespace paimon::orc
