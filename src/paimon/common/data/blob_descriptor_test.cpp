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

#include "paimon/common/data/blob_descriptor.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlobDescriptorTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        ASSERT_OK_AND_ASSIGN(descriptor_,
                             BlobDescriptor::Create("test_uri", /*offset=*/1024, /*length=*/2048));
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<BlobDescriptor> descriptor_;
};

TEST_F(BlobDescriptorTest, TestConstructorAndGetters) {
    ASSERT_EQ(descriptor_->Uri(), "test_uri");
    ASSERT_EQ(descriptor_->Offset(), 1024);
    ASSERT_EQ(descriptor_->Length(), 2048);
}

TEST_F(BlobDescriptorTest, TestDeserializeCompatibilityForJavaWithVersion1) {
    std::vector<char> bytes = {1, 8, 0, 0, 0, 116, 101, 115, 116, 95, 117, 114, 105, 0, 4,
                               0, 0, 0, 0, 0, 0,   0,   8,   0,   0,  0,   0,   0,   0};
    auto java_serialized = std::string(bytes.data(), bytes.size());

    ASSERT_OK_AND_ASSIGN(auto descriptor, BlobDescriptor::Deserialize(java_serialized.data(),
                                                                      java_serialized.size()));
    ASSERT_EQ(descriptor->Version(), (int8_t)1);
    ASSERT_EQ(descriptor->Uri(), "test_uri");
    ASSERT_EQ(descriptor->Offset(), 1024);
    ASSERT_EQ(descriptor->Length(), 2048);
}

TEST_F(BlobDescriptorTest, TestDeserializeCompatibilityForJavaWithVersion2) {
    std::vector<char> bytes = {2,   67,  83,  69,  68, 66,  79,  76,  66, 8, 0, 0, 0,
                               116, 101, 115, 116, 95, 117, 114, 105, 0,  4, 0, 0, 0,
                               0,   0,   0,   0,   8,  0,   0,   0,   0,  0, 0};
    auto java_serialized = std::string(bytes.data(), bytes.size());

    ASSERT_OK_AND_ASSIGN(auto descriptor, BlobDescriptor::Deserialize(java_serialized.data(),
                                                                      java_serialized.size()));
    ASSERT_EQ(descriptor->Version(), (int8_t)2);
    ASSERT_EQ(descriptor->Uri(), "test_uri");
    ASSERT_EQ(descriptor->Offset(), 1024);
    ASSERT_EQ(descriptor->Length(), 2048);

    PAIMON_UNIQUE_PTR<Bytes> cpp_serialized = descriptor->Serialize(pool_);
    auto cpp_serialized_string = std::string(cpp_serialized->data(), cpp_serialized->size());
    ASSERT_EQ(cpp_serialized_string, java_serialized);
}

TEST_F(BlobDescriptorTest, TestSerializeDeserializeWithEmptyUri) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> empty_uri_descriptor,
                         BlobDescriptor::Create(/*uri=*/"", /*offset=*/0, /*length=*/0));
    auto serialized = empty_uri_descriptor->Serialize(pool_);
    ASSERT_OK_AND_ASSIGN(auto restored_descriptor,
                         BlobDescriptor::Deserialize(serialized->data(), serialized->size()));

    ASSERT_EQ(restored_descriptor->Uri(), "");
    ASSERT_EQ(restored_descriptor->Offset(), 0);
    ASSERT_EQ(restored_descriptor->Length(), 0);
}

TEST_F(BlobDescriptorTest, TestSerializeDeserializeWithDynamicLength) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> empty_uri_descriptor,
                         BlobDescriptor::Create("test_uri", /*offset=*/0, /*length=*/-1));
    auto serialized = empty_uri_descriptor->Serialize(pool_);
    ASSERT_OK_AND_ASSIGN(auto restored_descriptor,
                         BlobDescriptor::Deserialize(serialized->data(), serialized->size()));

    ASSERT_EQ(restored_descriptor->Uri(), "test_uri");
    ASSERT_EQ(restored_descriptor->Offset(), 0);
    ASSERT_EQ(restored_descriptor->Length(), -1);
}

TEST_F(BlobDescriptorTest, TestInvalidParameters) {
    // Test deserialize invalid version
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> descriptor,
                             BlobDescriptor::Create(/*uri=*/"test", /*offset=*/1, /*length=*/2));
        auto serialized = descriptor->Serialize(pool_);
        (*serialized)[0] = '\x03';
        ASSERT_NOK_WITH_MSG(
            BlobDescriptor::Deserialize(serialized->data(), serialized->size()),
            "Expecting BlobDescriptor version to be less than or equal to 2, but found 3");
    }
    // Test deserialize invalid buffer size
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> descriptor,
                             BlobDescriptor::Create(/*uri=*/"test", /*offset=*/1, /*length=*/2));
        auto serialized = descriptor->Serialize(pool_);
        ASSERT_NOK(BlobDescriptor::Deserialize(serialized->data(), /*size=*/5));
    }
    // Test invalid offset
    {
        ASSERT_NOK_WITH_MSG(BlobDescriptor::Create(/*uri=*/"test", /*offset=*/-1, /*length=*/2),
                            "offset -1 is less than 0");
    }
    // Test invalid length
    {
        ASSERT_NOK_WITH_MSG(BlobDescriptor::Create(/*uri=*/"test", /*offset=*/1, /*length=*/-2),
                            "length -2 is less than -1");
    }
}

TEST_F(BlobDescriptorTest, TestToString) {
    std::string debug_str = descriptor_->ToString();
    ASSERT_FALSE(debug_str.empty());
    ASSERT_TRUE(debug_str.find("version=2") != std::string::npos);
    ASSERT_TRUE(debug_str.find("uri='test_uri'") != std::string::npos);
    ASSERT_TRUE(debug_str.find("offset=1024") != std::string::npos);
    ASSERT_TRUE(debug_str.find("length=2048") != std::string::npos);
}

TEST_F(BlobDescriptorTest, TestRoundTripConsistency) {
    auto first_serialized = descriptor_->Serialize(pool_);
    ASSERT_OK_AND_ASSIGN(
        auto first_restored,
        BlobDescriptor::Deserialize(first_serialized->data(), first_serialized->size()));
    auto second_serialized = first_restored->Serialize(pool_);
    ASSERT_EQ(*first_serialized, *second_serialized);

    ASSERT_OK_AND_ASSIGN(
        auto second_restored,
        BlobDescriptor::Deserialize(second_serialized->data(), second_serialized->size()));
    ASSERT_EQ(second_restored->Uri(), "test_uri");
    ASSERT_EQ(second_restored->Offset(), 1024);
    ASSERT_EQ(second_restored->Length(), 2048);
}

TEST_F(BlobDescriptorTest, TestIsBlobDescriptorWithValidDescriptor) {
    // A valid v2 descriptor should be recognized
    auto serialized = descriptor_->Serialize(pool_);
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobDescriptor::IsBlobDescriptor(serialized->data(), serialized->size()));
    ASSERT_TRUE(result);
}

TEST_F(BlobDescriptorTest, TestIsBlobDescriptorWithTooShortBuffer) {
    // Buffer shorter than 9 bytes should return false
    std::vector<char> short_buffer = {0x02, 0x43, 0x53, 0x45, 0x44, 0x42, 0x4F, 0x4C};
    ASSERT_OK_AND_ASSIGN(
        bool result, BlobDescriptor::IsBlobDescriptor(short_buffer.data(), short_buffer.size()));
    ASSERT_FALSE(result);

    // Empty buffer
    ASSERT_OK_AND_ASSIGN(bool empty_result, BlobDescriptor::IsBlobDescriptor(nullptr, 0));
    ASSERT_FALSE(empty_result);
}

TEST_F(BlobDescriptorTest, TestIsBlobDescriptorWithFutureVersion) {
    // Version > CURRENT_VERSION should return false (not an error)
    auto serialized = descriptor_->Serialize(pool_);
    (*serialized)[0] = '\x03';  // set version to 3 (> CURRENT_VERSION)
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobDescriptor::IsBlobDescriptor(serialized->data(), serialized->size()));
    ASSERT_FALSE(result);
}

TEST_F(BlobDescriptorTest, TestIsBlobDescriptorWithWrongMagic) {
    // Wrong magic number should return false
    auto serialized = descriptor_->Serialize(pool_);
    // Corrupt the magic bytes (bytes 1-8)
    (*serialized)[1] = '\x00';
    (*serialized)[2] = '\x00';
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobDescriptor::IsBlobDescriptor(serialized->data(), serialized->size()));
    ASSERT_FALSE(result);
}

TEST_F(BlobDescriptorTest, TestIsBlobDescriptorWithRandomData) {
    // Random data that doesn't match blob descriptor format
    std::vector<char> random_data = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobDescriptor::IsBlobDescriptor(random_data.data(), random_data.size()));
    ASSERT_FALSE(result);
}

TEST_F(BlobDescriptorTest, TestIsBlobDescriptorWithVersion1Data) {
    // v1 data: version=1, followed by uri_length (not magic), should return false
    // because reading bytes 1-8 as magic won't match MAGIC constant
    std::vector<char> v1_bytes = {1, 8, 0, 0, 0, 116, 101, 115, 116, 95, 117, 114, 105, 0, 4,
                                  0, 0, 0, 0, 0, 0,   0,   8,   0,   0,  0,   0,   0,   0};
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobDescriptor::IsBlobDescriptor(v1_bytes.data(), v1_bytes.size()));
    ASSERT_FALSE(result);
}

TEST_F(BlobDescriptorTest, TestIsBlobDescriptorWithExactly9Bytes) {
    // Exactly 9 bytes with valid version and magic should return true
    // version=2, magic=0x424C4F4244455343 in little-endian
    std::vector<char> minimal = {0x02, 0x43, 0x53, 0x45, 0x44, 0x42, 0x4F, 0x4C, 0x42};
    ASSERT_OK_AND_ASSIGN(bool result,
                         BlobDescriptor::IsBlobDescriptor(minimal.data(), minimal.size()));
    ASSERT_TRUE(result);
}

}  // namespace paimon::test
