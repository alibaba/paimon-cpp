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

#include "paimon/core/io/rolling_blob_file_writer.h"

#include <optional>
#include <string>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class RollingBlobFileWriterTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(RollingBlobFileWriterTest, ValidateFileConsistency) {
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/405, /*row_count=*/4,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("str_0"), 1}, {std::string("str_3"), 2},
                                          std::vector<int64_t>({0, 2}), pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt, /*first_row_id=*/0,
        /*write_cols=*/std::vector<std::string>({"f0", "f1"}));

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-xxx.blob", /*file_size=*/764, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, std::vector<int64_t>({-1}),
                                          pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt, /*first_row_id=*/0,
        /*write_cols=*/std::vector<std::string>({"blob"}));

    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-xxx.blob", /*file_size=*/3023, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, std::vector<int64_t>({-1}),
                                          pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt, /*first_row_id=*/3,
        /*write_cols=*/std::vector<std::string>({"blob"}));
    ASSERT_OK(RollingBlobFileWriter::ValidateFileConsistency(file_meta1, {file_meta2, file_meta3}));
    ASSERT_NOK_WITH_MSG(RollingBlobFileWriter::ValidateFileConsistency(file_meta1, {file_meta2}),
                        "This is a bug: The row count of main file and blob files does not match.");
}

}  // namespace paimon::test
