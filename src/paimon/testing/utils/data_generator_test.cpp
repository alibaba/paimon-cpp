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

#include "paimon/testing/utils/data_generator.h"

#include <map>
#include <variant>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class DataGeneratorTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    BinaryRow Make(const RowKind* kind, const std::string& f0, const std::string& f1,
                   const std::string& f2) {
        return BinaryRowGenerator::GenerateRow(kind, {f0, f1, f2}, pool_.get());
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(DataGeneratorTest, TestSimple) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::utf8()),
                                 arrow::field("f2", arrow::utf8())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options;
    options[Options::BUCKET_KEY] = "f0";
    options[Options::BUCKET] = "2";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    DataGenerator gen(table_schema, GetDefaultPool());

    std::vector<BinaryRow> rows;
    rows.push_back(Make(RowKind::Insert(), "Alex", "20250326", "18"));
    rows.push_back(Make(RowKind::Insert(), "Bob", "20250326", "19"));
    rows.push_back(Make(RowKind::Insert(), "Cathy", "20250325", "20"));
    rows.push_back(Make(RowKind::Insert(), "David", "20250325", "21"));
    rows.push_back(Make(RowKind::Insert(), "Evan", "20250326", "22"));
    rows.push_back(Make(RowKind::Delete(), "Alex", "20250326", "18"));
    rows.push_back(Make(RowKind::Delete(), "Bob", "20250326", "19"));
    ASSERT_OK_AND_ASSIGN(auto batches, gen.SplitArrayByPartitionAndBucket(rows));
    ASSERT_EQ(3, batches.size());
}

}  // namespace paimon::test
