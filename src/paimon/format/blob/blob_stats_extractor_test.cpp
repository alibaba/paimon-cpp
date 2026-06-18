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

#include "paimon/format/blob/blob_stats_extractor.h"

#include <cstdint>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/defs.h"
#include "paimon/format/column_stats.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {

class BlobStatsExtractorTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        // Create a blob schema with one blob field
        blob_field_ = BlobUtils::ToArrowField("blob_field", false);
        blob_schema_ = arrow::schema({blob_field_});
        fs_ = std::make_shared<LocalFileSystem>();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Field> blob_field_;
    std::shared_ptr<arrow::Schema> blob_schema_;
    std::shared_ptr<FileSystem> fs_;
};

TEST_F(BlobStatsExtractorTest, TestDifferentBlobFiles) {
    BlobStatsExtractor extractor(blob_schema_);

    std::vector<std::pair<std::string, int64_t>> test_files = {
        {"data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob", 3},
        {"data-d7816e8e-6c6d-4e28-9137-837cdf706350-2.blob", 2},
        {"data-d7816e8e-6c6d-4e28-9137-837cdf706350-3.blob", 4},
        {"data-d7816e8e-6c6d-4e28-9137-837cdf706350-4.blob", 1}};

    for (const auto& [filename, expected_rows] : test_files) {
        std::string blob_file_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/bucket-0/" + filename;

        ASSERT_OK_AND_ASSIGN(auto stats_with_info,
                             extractor.ExtractWithFileInfo(fs_, blob_file_path, pool_));

        // Check stats structure
        ASSERT_EQ(1u, stats_with_info.first.size());
        ASSERT_TRUE(stats_with_info.first[0]);
        ASSERT_EQ(FieldType::STRING, stats_with_info.first[0]->GetFieldType());
        ASSERT_EQ("min null, max null, null count null", stats_with_info.first[0]->ToString());

        // Check row count matches expected
        ASSERT_EQ(expected_rows, stats_with_info.second.GetRowCount())
            << "Row count mismatch for file: " << filename;
    }
}

TEST_F(BlobStatsExtractorTest, TestInvalidCase) {
    std::string blob_file_path = paimon::test::GetDataDir() +
                                 "/db_with_blob.db/table_with_blob/bucket-0/"
                                 "data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob";

    // Should fail because schema has more than 1 field
    {
        auto int_field = arrow::field("int_field", arrow::int32());
        auto multi_field_schema = arrow::schema({blob_field_, int_field});
        BlobStatsExtractor extractor(multi_field_schema);
        ASSERT_NOK_WITH_MSG(extractor.ExtractWithFileInfo(fs_, blob_file_path, pool_),
                            "schema field number 2 is not 1");
    }
    // Should fail because field is not a blob field
    {
        auto string_field = arrow::field("string_field", arrow::utf8());
        auto non_blob_schema = arrow::schema({string_field});
        BlobStatsExtractor extractor(non_blob_schema);
        ASSERT_NOK_WITH_MSG(extractor.ExtractWithFileInfo(fs_, blob_file_path, pool_),
                            "field string_field: string is not BLOB");
    }
    // Should fail because file doesn't exist
    {
        BlobStatsExtractor extractor(blob_schema_);
        std::string non_existent_path = "/path/that/does/not/exist.blob";
        ASSERT_NOK_WITH_MSG(extractor.ExtractWithFileInfo(fs_, non_existent_path, pool_),
                            "File '/path/that/does/not/exist.blob' not exists");
    }
}

}  // namespace paimon::blob::test
