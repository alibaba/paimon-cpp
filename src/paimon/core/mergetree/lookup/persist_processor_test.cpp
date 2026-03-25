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

#include "paimon/core/mergetree/lookup/persist_processor.h"

#include "gtest/gtest.h"
#include "paimon/core/mergetree/lookup/default_lookup_serializer_factory.h"
#include "paimon/core/mergetree/lookup/persist_empty_processor.h"
#include "paimon/core/mergetree/lookup/persist_position_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_and_pos_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_processor.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class PersistProcessorTest : public testing::Test {
 public:
    void CheckResult(const KeyValue& kv) {
        ASSERT_EQ(kv_.key, kv.key);

        ASSERT_EQ(kv.value->GetFieldCount(), 4);
        ASSERT_FALSE(kv.value->IsNullAt(0));
        ASSERT_EQ(kv.value->GetString(0).ToString(), std::string("Alice"));
        ASSERT_EQ(kv.value->GetInt(1), 10);
        ASSERT_TRUE(kv.value->IsNullAt(2));
        ASSERT_EQ(kv.value->GetDouble(3), 10.1);

        ASSERT_EQ(kv_.level, kv.level);
        ASSERT_EQ(kv_.sequence_number, kv.sequence_number);
        ASSERT_EQ(*kv_.value_kind, *kv.value_kind);
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    KeyValue kv_ = KeyValue(RowKind::Insert(), /*sequence_number=*/500, /*level=*/4, /*key=*/
                            BinaryRowGenerator::GenerateRowPtr({10}, pool_.get()),
                            /*value=*/
                            BinaryRowGenerator::GenerateRowPtr(
                                {std::string("Alice"), 10, NullType(), 10.1}, pool_.get()));
    std::shared_ptr<arrow::Schema> file_schema_ =
        arrow::schema({arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                       arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())});
    std::shared_ptr<LookupSerializerFactory> serializer_factory_ =
        std::make_shared<DefaultLookupSerializerFactory>();
    std::string file_name_ = "test.file";
};

TEST_F(PersistProcessorTest, TestEmptyProcessor) {
    auto processor_factory = std::make_shared<PersistEmptyProcessor::Factory>();
    ASSERT_EQ(processor_factory->Identifier(), "empty");
    ASSERT_OK_AND_ASSIGN(auto processor,
                         processor_factory->Create(serializer_factory_->Version(),
                                                   /*serializer_factory=*/serializer_factory_,
                                                   /*file_schema=*/file_schema_, pool_));
    ASSERT_FALSE(processor->WithPosition());
    ASSERT_OK_AND_ASSIGN(auto bytes, processor->PersistToDisk(kv_));
    ASSERT_EQ(bytes->size(), 0);
    ASSERT_OK_AND_ASSIGN(bool value,
                         processor->ReadFromDisk(kv_.key, kv_.level, bytes, file_name_));
    ASSERT_TRUE(value);

    ASSERT_NOK_WITH_MSG(processor->PersistToDisk(kv_, /*row_position=*/30),
                        "Not support for PersistToDisk with position");
}

TEST_F(PersistProcessorTest, TestPositionProcessor) {
    auto processor_factory = std::make_shared<PersistPositionProcessor::Factory>();
    ASSERT_EQ(processor_factory->Identifier(), "position");
    ASSERT_OK_AND_ASSIGN(auto processor,
                         processor_factory->Create(serializer_factory_->Version(),
                                                   /*serializer_factory=*/serializer_factory_,
                                                   /*file_schema=*/file_schema_, pool_));
    ASSERT_TRUE(processor->WithPosition());
    ASSERT_OK_AND_ASSIGN(auto bytes, processor->PersistToDisk(kv_, /*row_position=*/30));
    ASSERT_OK_AND_ASSIGN(FilePosition file_position,
                         processor->ReadFromDisk(kv_.key, kv_.level, bytes, file_name_));
    ASSERT_EQ(file_position.file_name, file_name_);
    ASSERT_EQ(file_position.row_position, 30);

    ASSERT_NOK_WITH_MSG(processor->PersistToDisk(kv_),
                        "invalid operation, do not support persist to disk without position in "
                        "PersistPositionProcessor");
}

TEST_F(PersistProcessorTest, TestValueProcessor) {
    auto processor_factory = std::make_shared<PersistValueProcessor::Factory>(file_schema_);
    ASSERT_EQ(processor_factory->Identifier(), "value");
    ASSERT_OK_AND_ASSIGN(auto processor,
                         processor_factory->Create(serializer_factory_->Version(),
                                                   /*serializer_factory=*/serializer_factory_,
                                                   /*file_schema=*/file_schema_, pool_));
    ASSERT_FALSE(processor->WithPosition());
    ASSERT_OK_AND_ASSIGN(auto bytes, processor->PersistToDisk(kv_));
    ASSERT_OK_AND_ASSIGN(KeyValue kv,
                         processor->ReadFromDisk(kv_.key, kv_.level, bytes, file_name_));
    CheckResult(kv);

    ASSERT_NOK_WITH_MSG(processor->PersistToDisk(kv_, /*row_position=*/30),
                        "Not support for PersistToDisk with position");
}

TEST_F(PersistProcessorTest, TestInvalideValueProcessor) {
    std::shared_ptr<arrow::Schema> current_schema =
        arrow::schema({arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                       arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float32())});

    auto processor_factory = std::make_shared<PersistValueProcessor::Factory>(current_schema);
    ASSERT_EQ(processor_factory->Identifier(), "value");
    // test schema not equal
    ASSERT_NOK_WITH_MSG(processor_factory->Create(serializer_factory_->Version(),
                                                  /*serializer_factory=*/serializer_factory_,
                                                  /*file_schema=*/file_schema_, pool_),
                        "f3: float must be equal with file_schema");
    // test version mismatch
    ASSERT_NOK_WITH_MSG(
        processor_factory->Create("invalid version",
                                  /*serializer_factory=*/serializer_factory_,
                                  /*file_schema=*/current_schema, pool_),
        "file_ser_version invalid version mismatch DefaultLookupSerializerFactory version v1");
}

TEST_F(PersistProcessorTest, TestValueAndPositionProcessor) {
    auto processor_factory = std::make_shared<PersistValueAndPosProcessor::Factory>(file_schema_);
    ASSERT_EQ(processor_factory->Identifier(), "position-and-value");
    ASSERT_OK_AND_ASSIGN(auto processor,
                         processor_factory->Create(serializer_factory_->Version(),
                                                   /*serializer_factory=*/serializer_factory_,
                                                   /*file_schema=*/file_schema_, pool_));
    ASSERT_TRUE(processor->WithPosition());
    ASSERT_OK_AND_ASSIGN(auto bytes, processor->PersistToDisk(kv_, /*row_position=*/30));
    ASSERT_OK_AND_ASSIGN(PositionedKeyValue pos_kv,
                         processor->ReadFromDisk(kv_.key, kv_.level, bytes, file_name_));
    CheckResult(pos_kv.key_value);
    ASSERT_EQ(pos_kv.file_name, file_name_);
    ASSERT_EQ(pos_kv.row_position, 30);

    ASSERT_NOK_WITH_MSG(processor->PersistToDisk(kv_),
                        "invalid operation, do not support persist to disk without position in "
                        "PersistValueAndPosProcessor");
}
}  // namespace paimon::test
