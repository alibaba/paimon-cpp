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

#include "paimon/common/data/serializer/binary_row_serializer.h"

#include <string>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/memory/memory_segment.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BinaryRowSerializerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        serializer_ = std::make_unique<BinaryRowSerializer>(3, pool_);
    }

    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<BinaryRowSerializer> serializer_;
};

TEST_F(BinaryRowSerializerTest, SerializeAndDeserialize) {
    // Create a BinaryRow with some test data
    BinaryRow row(3);
    BinaryRowWriter writer(&row, 20, pool_.get());
    writer.WriteInt(0, 42);
    writer.WriteString(1, BinaryString::FromString("test", pool_.get()));
    writer.WriteDouble(2, 3.14);

    // Serialize the BinaryRow
    MemorySegmentOutputStream output_stream(1, pool_);
    ASSERT_OK(serializer_->Serialize(row, &output_stream));
    auto segments = output_stream.Segments();
    auto bytes =
        MemorySegmentUtils::CopyToBytes(segments, 0, output_stream.CurrentSize(), pool_.get());

    // Deserialize the BinaryRow
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream in(input_stream);
    ASSERT_OK_AND_ASSIGN(BinaryRow deserialized_row, serializer_->Deserialize(&in));
    EXPECT_EQ(deserialized_row.GetInt(0), 42);
    EXPECT_EQ(deserialized_row.GetString(1).ToString(), "test");
    EXPECT_EQ(deserialized_row.GetDouble(2), 3.14);
}

}  // namespace paimon::test
