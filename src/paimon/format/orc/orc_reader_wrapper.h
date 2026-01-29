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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/memory_pool.h"
#include "fmt/format.h"
#include "paimon/format/orc/orc_adapter.h"
#include "paimon/format/orc/read_range_generator.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"

namespace paimon::orc {

// The OrcReaderWrapper is a decorator class designed to support GetNextRowToRead.
class OrcReaderWrapper {
 public:
    ~OrcReaderWrapper() {
        row_reader_.reset();
        reader_.reset();
    }

    static Result<std::unique_ptr<OrcReaderWrapper>> Create(
        std::unique_ptr<::orc::Reader> reader, const std::string& file_name, int32_t batch_size,
        uint64_t natural_read_size, const std::map<std::string, std::string>& options,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
        const std::shared_ptr<::orc::MemoryPool>& orc_pool) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<ReadRangeGenerator> range_generator,
            ReadRangeGenerator::Create(reader.get(), natural_read_size, options));
        return std::unique_ptr<OrcReaderWrapper>(
            new OrcReaderWrapper(std::move(reader), std::move(range_generator), file_name,
                                 batch_size, arrow_pool, orc_pool));
    }

    Status SeekToRow(uint64_t row_number);
    Result<BatchReader::ReadBatch> Next();
    Status SetReadSchema(const std::shared_ptr<arrow::DataType>& target_type,
                         const ::orc::RowReaderOptions& row_reader_options);

    uint64_t GetNextRowToRead() const {
        return next_row_;
    }

    uint64_t GetRowNumber() const {
        return row_reader_->getRowNumber();
    }

    uint64_t GetNumberOfRows() const {
        return reader_->getNumberOfRows();
    }

    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) {
        // Intentionally a no-op: SetReadRanges is a best-effort hint only.
        return Status::OK();
    }

    const ::orc::Type& GetOrcType() const {
        return reader_->getType();
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        std::vector<uint64_t> target_column_ids, uint64_t begin_row_num, uint64_t end_row_num,
        bool* need_prefetch) const {
        return range_generator_->GenReadRanges(target_column_ids, begin_row_num, end_row_num,
                                               need_prefetch);
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> PreBufferRange(
        const std::vector<uint64_t>& target_column_ids) {
        uint64_t stripe_number = reader_->getNumberOfStripes();
        std::vector<uint32_t> stripes;
        for (uint64_t i = 0; i < stripe_number; i++) {
            stripes.push_back(static_cast<uint32_t>(i));
        }
        std::list<uint64_t> include_types(target_column_ids.begin(), target_column_ids.end());
        try {
            return reader_->preBufferRange(stripes, include_types);
        } catch (const std::exception& e) {
            return Status::Invalid(fmt::format(
                "orc pre buffer range failed for file {}, with {} error", file_name_, e.what()));
        } catch (...) {
            return Status::Invalid(fmt::format(
                "orc pre buffer range failed for file {}, with unknown error", file_name_));
        }
    }

 private:
    OrcReaderWrapper(std::unique_ptr<::orc::Reader> reader,
                     std::unique_ptr<ReadRangeGenerator> range_generator,
                     const std::string& file_name, int32_t batch_size,
                     const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                     const std::shared_ptr<::orc::MemoryPool>& orc_pool)
        : reader_(std::move(reader)),
          range_generator_(std::move(range_generator)),
          file_name_(file_name),
          batch_size_(batch_size),
          arrow_pool_(arrow_pool),
          orc_pool_(orc_pool) {}

    std::unique_ptr<::orc::Reader> reader_;
    std::unique_ptr<::orc::RowReader> row_reader_;

    std::unique_ptr<ReadRangeGenerator> range_generator_;

    const std::string file_name_;
    const int32_t batch_size_;

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<::orc::MemoryPool> orc_pool_;

    std::shared_ptr<arrow::DataType> target_type_;

    // The next absolute row index to read.
    uint64_t next_row_ = 0;
    bool has_error_ = false;
};

}  // namespace paimon::orc
