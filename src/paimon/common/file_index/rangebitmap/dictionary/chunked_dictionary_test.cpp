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

#include "paimon/common/file_index/rangebitmap/dictionary/chunked_dictionary.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/common/file_index/rangebitmap/utils/literal_serialization_utils.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/defs.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ChunkedDictionaryTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        pool_.reset();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(ChunkedDictionaryTest, TestEmptyDictionary) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::FLOAT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));

    // Serialize empty dictionary
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());

    // Create dictionary from bytes
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::FLOAT, input_stream, 0, pool_));

    // Test finding non-existent key
    ASSERT_OK_AND_ASSIGN(auto result, dict->Find(Literal(42)));
    ASSERT_EQ(result, -1);  // Should return -1 for not found
    ASSERT_NOK_WITH_MSG(dict->Find(0), "Cannot find code 0 in an empty Dictionary");
}

// when chunk can't even fit in a single key, it still works
// every key will be stored as representative key in chunk header
TEST_F(ChunkedDictionaryTest, TestChunkSizeTooSmall) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::FLOAT));
    // set chunk_size_bytes_limit to only 1
    ASSERT_OK_AND_ASSIGN(auto appender, ChunkedDictionary::Appender::Create(key_factory, 1, pool_));
    ASSERT_OK(appender->AppendSorted(Literal(1.1f), 0));
    ASSERT_OK(appender->AppendSorted(Literal(2.2f), 1));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::FLOAT, input_stream, 0, pool_));
    ASSERT_OK_AND_ASSIGN(auto code, dict->Find(Literal(1.1f)));
    ASSERT_EQ(code, 0);
}

// NaN == NaN -> true
// -infinity < -0.0 < +0.0 < +infinity < NaN == NaN
TEST_F(ChunkedDictionaryTest, TestNaNFloatBehavior) {
    constexpr float nan = std::numeric_limits<float>::quiet_NaN();
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::FLOAT));
    {
        ASSERT_OK_AND_ASSIGN(auto appender,
                             ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
        // append NaN
        ASSERT_OK(appender->AppendSorted(Literal(nan), 0));
        // append NaN again, Sorted check should fail
        ASSERT_NOK_WITH_MSG(appender->AppendSorted(Literal(nan), 0), "key must be in sorted order");
    }
    {
        // 1.0, +infinity, NaN
        ASSERT_OK_AND_ASSIGN(auto appender,
                             ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
        ASSERT_OK(appender->AppendSorted(Literal(1.0f), 0));
        // should pass the sorted check
        ASSERT_OK(appender->AppendSorted(Literal(std::numeric_limits<float>::infinity()), 1));
        ASSERT_OK(appender->AppendSorted(Literal(nan), 2));
        ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
        auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
        ASSERT_OK_AND_ASSIGN(auto dict,
                             ChunkedDictionary::Create(FieldType::FLOAT, input_stream, 0, pool_));
        ASSERT_OK_AND_ASSIGN(int32_t nan_code, dict->Find(Literal(nan)));
        ASSERT_OK_AND_ASSIGN(int32_t one_code, dict->Find(Literal(1.0f)));
        ASSERT_EQ(nan_code, 2);
        ASSERT_EQ(one_code, 0);
    }
}

// +0.0f and -0.0f are considered different
// -0.0f < +0.0f
TEST_F(ChunkedDictionaryTest, TestFloatSignedZeroDictionary) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::FLOAT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));

    // Build dictionary with a single -0.0f key
    ASSERT_OK(appender->AppendSorted(Literal(-0.0f), 0));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::FLOAT, input_stream, 0, pool_));

    // -0.0f is present and maps to code 0
    ASSERT_OK_AND_ASSIGN(int32_t code_neg, dict->Find(Literal(-0.0f)));
    ASSERT_EQ(code_neg, 0);

    // +0.0f is not present and should return a negative insertion point (Java-compatible behavior)
    ASSERT_OK_AND_ASSIGN(int32_t code_pos, dict->Find(Literal(0.0f)));
    ASSERT_LT(code_pos, 0);
}

TEST_F(ChunkedDictionaryTest, TestFloatInfinity) {
    // Test float infinity values
    ASSERT_OK_AND_ASSIGN(auto float_factory, KeyFactory::Create(FieldType::FLOAT));
    ASSERT_OK_AND_ASSIGN(auto float_appender,
                         ChunkedDictionary::Appender::Create(float_factory, 1024, pool_));

    // Add positive infinity, negative infinity, and regular float
    ASSERT_OK(float_appender->AppendSorted(Literal(-std::numeric_limits<float>::infinity()), 0));
    ASSERT_OK(float_appender->AppendSorted(Literal(42.5f), 1));
    ASSERT_OK(float_appender->AppendSorted(Literal(std::numeric_limits<float>::infinity()), 2));

    // Serialize and recreate
    ASSERT_OK_AND_ASSIGN(auto float_bytes, float_appender->Serialize());
    auto float_input_stream =
        std::make_shared<ByteArrayInputStream>(float_bytes->data(), float_bytes->size());
    ASSERT_OK_AND_ASSIGN(auto float_dict,
                         ChunkedDictionary::Create(FieldType::FLOAT, float_input_stream, 0, pool_));

    // Test finding infinity values
    ASSERT_OK_AND_ASSIGN(auto pos_inf_result,
                         float_dict->Find(Literal(std::numeric_limits<float>::infinity())));
    ASSERT_EQ(pos_inf_result, 2);

    ASSERT_OK_AND_ASSIGN(auto neg_inf_result,
                         float_dict->Find(Literal(-std::numeric_limits<float>::infinity())));
    ASSERT_EQ(neg_inf_result, 0);

    ASSERT_OK_AND_ASSIGN(auto regular_result, float_dict->Find(Literal(42.5f)));
    ASSERT_EQ(regular_result, 1);

    // Test double infinity values
    ASSERT_OK_AND_ASSIGN(auto double_factory, KeyFactory::Create(FieldType::DOUBLE));
    ASSERT_OK_AND_ASSIGN(auto double_appender,
                         ChunkedDictionary::Appender::Create(double_factory, 1024, pool_));

    ASSERT_OK(double_appender->AppendSorted(Literal(-std::numeric_limits<double>::infinity()), 0));
    ASSERT_OK(double_appender->AppendSorted(Literal(std::numeric_limits<double>::infinity()), 1));

    ASSERT_OK_AND_ASSIGN(auto double_bytes, double_appender->Serialize());
    auto double_input_stream =
        std::make_shared<ByteArrayInputStream>(double_bytes->data(), double_bytes->size());
    ASSERT_OK_AND_ASSIGN(auto double_dict, ChunkedDictionary::Create(
                                               FieldType::DOUBLE, double_input_stream, 0, pool_));

    ASSERT_OK_AND_ASSIGN(auto double_pos_inf_result,
                         double_dict->Find(Literal(std::numeric_limits<double>::infinity())));
    ASSERT_EQ(double_pos_inf_result, 1);

    ASSERT_OK_AND_ASSIGN(auto double_neg_inf_result,
                         double_dict->Find(Literal(-std::numeric_limits<double>::infinity())));
    ASSERT_EQ(double_neg_inf_result, 0);
}

TEST_F(ChunkedDictionaryTest, TestSingleChunk) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));

    // Add some sorted keys
    ASSERT_OK(appender->AppendSorted(Literal(10), 0));
    ASSERT_OK(appender->AppendSorted(Literal(20), 1));
    ASSERT_OK(appender->AppendSorted(Literal(30), 2));

    // Serialize and recreate
    auto bytes = appender->Serialize();
    ASSERT_TRUE(bytes.ok());

    auto input_stream =
        std::make_shared<ByteArrayInputStream>(bytes.value()->data(), bytes.value()->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::INT, input_stream, 0, pool_));

    // Test finding existing keys
    ASSERT_OK_AND_ASSIGN(auto result1, dict->Find(Literal(10)));
    ASSERT_EQ(result1, 0);

    ASSERT_OK_AND_ASSIGN(auto result2, dict->Find(Literal(20)));
    ASSERT_EQ(result2, 1);

    ASSERT_OK_AND_ASSIGN(auto result3, dict->Find(Literal(30)));
    ASSERT_EQ(result3, 2);

    // Test finding non-existent key
    ASSERT_OK_AND_ASSIGN(auto result4, dict->Find(Literal(25)));
    ASSERT_EQ(result4, -3);  // Should be negative, insertion_point = 2, between 20 and 30
}

TEST_F(ChunkedDictionaryTest, TestMultiChunk) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(
                             key_factory, 64, pool_));  // Small chunk size to force multiple chunks

    // Add many keys to force chunk splitting
    for (int32_t i = 0; i < 20; ++i) {
        ASSERT_OK(appender->AppendSorted(Literal(i * 10), i));
    }

    // Serialize and recreate
    auto bytes = appender->Serialize();
    ASSERT_TRUE(bytes.ok());

    auto input_stream =
        std::make_shared<ByteArrayInputStream>(bytes.value()->data(), bytes.value()->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::INT, input_stream, 0, pool_));

    // Test finding keys across different chunks
    ASSERT_OK_AND_ASSIGN(auto result1, dict->Find(Literal(0)));
    ASSERT_EQ(result1, 0);

    ASSERT_OK_AND_ASSIGN(auto result2, dict->Find(Literal(50)));
    ASSERT_EQ(result2, 5);

    ASSERT_OK_AND_ASSIGN(auto result3, dict->Find(Literal(190)));
    ASSERT_EQ(result3, 19);

    // Test finding key that doesn't exist
    ASSERT_OK_AND_ASSIGN(auto result4, dict->Find(Literal(25)));
    ASSERT_EQ(result4,
              -4);  // Should be negative (not found)
}

TEST_F(ChunkedDictionaryTest, TestFindByCode) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));

    // Add some keys
    ASSERT_OK(appender->AppendSorted(Literal(10), 0));
    ASSERT_OK(appender->AppendSorted(Literal(20), 1));
    ASSERT_OK(appender->AppendSorted(Literal(30), 2));

    // Serialize and recreate
    auto bytes = appender->Serialize();
    ASSERT_TRUE(bytes.ok());

    auto input_stream =
        std::make_shared<ByteArrayInputStream>(bytes.value()->data(), bytes.value()->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::INT, input_stream, 0, pool_));

    // Test finding by code
    ASSERT_OK_AND_ASSIGN(auto result1, dict->Find(0));
    ASSERT_EQ(result1, Literal(10));

    ASSERT_OK_AND_ASSIGN(auto result2, dict->Find(1));
    ASSERT_EQ(result2, Literal(20));

    ASSERT_OK_AND_ASSIGN(auto result3, dict->Find(2));
    ASSERT_EQ(result3, Literal(30));
}

TEST_F(ChunkedDictionaryTest, TestUnsortedKeys) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));

    // Add keys in sorted order first
    ASSERT_OK(appender->AppendSorted(Literal(10), 0));
    ASSERT_OK(appender->AppendSorted(Literal(20), 1));

    // Try to add unsorted key should fail
    ASSERT_NOK_WITH_MSG(appender->AppendSorted(Literal(15), 2), "key must be in sorted order");
}

TEST_F(ChunkedDictionaryTest, TestInvalidCodes) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));

    // Add keys with duplicate codes - should fail
    ASSERT_OK(appender->AppendSorted(Literal(10), 1));
    ASSERT_OK(appender->AppendSorted(Literal(20), 2));

    // Try to add same code - should fail
    ASSERT_NOK_WITH_MSG(appender->AppendSorted(Literal(30), 2), "code must be in sorted order");

    // Last append failed, appender is not broken, still works
    ASSERT_OK(appender->AppendSorted(Literal(30), 3));

    // Code is not incrementing by step one should fail
    ASSERT_NOK_WITH_MSG(appender->AppendSorted(Literal(40), 5), "code must be in sorted order");
    auto bytes = appender->Serialize();
    ASSERT_TRUE(bytes.ok());

    auto input_stream =
        std::make_shared<ByteArrayInputStream>(bytes.value()->data(), bytes.value()->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::INT, input_stream, 0, pool_));
    ASSERT_NOK_WITH_MSG(dict->Find(0), "Invalid: Cannot find code 0");
}

TEST_F(ChunkedDictionaryTest, TestNullKey) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));

    // Null keys should be rejected
    ASSERT_NOK_WITH_MSG(appender->AppendSorted(Literal(FieldType::INT), 0),
                        "key should not be null");
}

TEST_F(ChunkedDictionaryTest, TestBoolean) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::BOOLEAN));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
    ASSERT_OK(appender->AppendSorted(Literal(false), 0));
    ASSERT_OK(appender->AppendSorted(Literal(true), 1));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::BOOLEAN, input_stream, 0, pool_));
    ASSERT_OK_AND_ASSIGN(int32_t code_true, dict->Find(Literal(true)));
    ASSERT_EQ(code_true, 1);
    ASSERT_OK_AND_ASSIGN(int32_t code_false, dict->Find(Literal(false)));
    ASSERT_EQ(code_false, 0);
    ASSERT_OK_AND_ASSIGN(Literal lit0, dict->Find(1));
    ASSERT_EQ(lit0, Literal(true));
    ASSERT_OK_AND_ASSIGN(Literal lit1, dict->Find(0));
    ASSERT_EQ(lit1, Literal(false));
}

TEST_F(ChunkedDictionaryTest, TestTinyInt) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::TINYINT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
    ASSERT_OK(appender->AppendSorted(Literal(static_cast<int8_t>(-10)), 0));
    ASSERT_OK(appender->AppendSorted(Literal(static_cast<int8_t>(100)), 1));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::TINYINT, input_stream, 0, pool_));
    ASSERT_OK_AND_ASSIGN(int32_t c0, dict->Find(Literal(static_cast<int8_t>(-10))));
    ASSERT_EQ(c0, 0);
    ASSERT_OK_AND_ASSIGN(Literal lit1, dict->Find(1));
    ASSERT_EQ(lit1, Literal(static_cast<int8_t>(100)));
}

TEST_F(ChunkedDictionaryTest, TestSmallInt) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::SMALLINT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
    ASSERT_OK(appender->AppendSorted(Literal(static_cast<int16_t>(-1000)), 0));
    ASSERT_OK(appender->AppendSorted(Literal(static_cast<int16_t>(2000)), 1));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::SMALLINT, input_stream, 0, pool_));
    ASSERT_OK_AND_ASSIGN(int32_t c0, dict->Find(Literal(static_cast<int16_t>(-1000))));
    ASSERT_EQ(c0, 0);
    ASSERT_OK_AND_ASSIGN(Literal lit1, dict->Find(1));
    ASSERT_EQ(lit1, Literal(static_cast<int16_t>(2000)));
}

TEST_F(ChunkedDictionaryTest, TestDouble) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::DOUBLE));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
    ASSERT_OK(appender->AppendSorted(Literal(static_cast<double>(-1000.3)), 0));
    ASSERT_OK(appender->AppendSorted(Literal(static_cast<double>(2000.8)), 1));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::DOUBLE, input_stream, 0, pool_));
    ASSERT_OK_AND_ASSIGN(int32_t c0, dict->Find(Literal(static_cast<double>(-1000.3))));
    ASSERT_EQ(c0, 0);
    ASSERT_OK_AND_ASSIGN(Literal lit1, dict->Find(1));
    ASSERT_EQ(lit1, Literal(static_cast<double>(2000.8)));
}

TEST_F(ChunkedDictionaryTest, TestDate) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::DATE));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
    constexpr int32_t date1 = 19725;
    constexpr int32_t date2 = 20000;
    ASSERT_OK(appender->AppendSorted(Literal(FieldType::DATE, date1), 0));
    ASSERT_OK(appender->AppendSorted(Literal(FieldType::DATE, date2), 1));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::DATE, input_stream, 0, pool_));
    ASSERT_OK_AND_ASSIGN(int32_t c0, dict->Find(Literal(FieldType::DATE, date1)));
    ASSERT_EQ(c0, 0);
    ASSERT_OK_AND_ASSIGN(Literal lit1, dict->Find(1));
    ASSERT_EQ(lit1.GetValue<int32_t>(), date2);
    ASSERT_EQ(lit1.GetType(), FieldType::DATE);
}

TEST_F(ChunkedDictionaryTest, TestBigInt) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::BIGINT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
    ASSERT_OK(appender->AppendSorted(Literal(static_cast<int64_t>(100000000000)), 0));
    ASSERT_OK(appender->AppendSorted(Literal(static_cast<int64_t>(200000000000)), 1));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::BIGINT, input_stream, 0, pool_));
    ASSERT_OK_AND_ASSIGN(int32_t c0, dict->Find(Literal(static_cast<int64_t>(100000000000))));
    ASSERT_EQ(c0, 0);
    ASSERT_OK_AND_ASSIGN(Literal lit1, dict->Find(1));
    ASSERT_EQ(lit1.GetValue<int64_t>(), 200000000000);
    ASSERT_EQ(lit1.GetType(), FieldType::BIGINT);
}

TEST_F(ChunkedDictionaryTest, TestKeyFactoryUnsupportedType) {
    ASSERT_NOK_WITH_MSG(KeyFactory::Create(FieldType::BINARY),
                        "Unsupported field type for KeyFactory: BINARY");
}

TEST_F(ChunkedDictionaryTest, TestStringKeyFactoryNotImplemented) {
    ASSERT_NOK_WITH_MSG(KeyFactory::Create(FieldType::STRING),
                        "Unsupported field type for KeyFactory: STRING");
}

TEST_F(ChunkedDictionaryTest, TestFindByCodeInvalidNegative) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
    ASSERT_OK(appender->AppendSorted(Literal(10), 0));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::INT, input_stream, 0, pool_));
    ASSERT_NOK_WITH_MSG(dict->Find(-1), "Invalid code: -1");
    // should fail reading invalid code
}

TEST_F(ChunkedDictionaryTest, TestGetChunkInvalidIndex) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024, pool_));
    ASSERT_OK(appender->AppendSorted(Literal(10), 0));
    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::INT, input_stream, 0, pool_));
    ASSERT_OK_AND_ASSIGN(auto chunk, dict->GetChunk(0));
    ASSERT_EQ(chunk->Offset(), 0);
    // should fail reading invalid chunk index
    ASSERT_NOK_WITH_MSG(dict->GetChunk(-1), "Invalid chunk index: -1");
    ASSERT_NOK_WITH_MSG(dict->GetChunk(1), "Invalid chunk index: 1");
}

TEST_F(ChunkedDictionaryTest, TestLiteralSerDeUtilsGetFixedFieldSize) {
    ASSERT_EQ(LiteralSerDeUtils::GetFixedFieldSize(FieldType::BOOLEAN).value(), sizeof(int8_t));
    ASSERT_EQ(LiteralSerDeUtils::GetFixedFieldSize(FieldType::TINYINT).value(), sizeof(int8_t));
    ASSERT_EQ(LiteralSerDeUtils::GetFixedFieldSize(FieldType::SMALLINT).value(), sizeof(int16_t));
    ASSERT_EQ(LiteralSerDeUtils::GetFixedFieldSize(FieldType::INT).value(), sizeof(int32_t));
    ASSERT_EQ(LiteralSerDeUtils::GetFixedFieldSize(FieldType::DATE).value(), sizeof(int32_t));
    ASSERT_EQ(LiteralSerDeUtils::GetFixedFieldSize(FieldType::BIGINT).value(), sizeof(int64_t));
    ASSERT_EQ(LiteralSerDeUtils::GetFixedFieldSize(FieldType::FLOAT).value(), sizeof(float));
    ASSERT_EQ(LiteralSerDeUtils::GetFixedFieldSize(FieldType::DOUBLE).value(), sizeof(double));
    // Unsupported types return false
    ASSERT_NOK_WITH_MSG(LiteralSerDeUtils::GetFixedFieldSize(FieldType::STRING),
                        "Unsupported field type for GetFixedFieldSize: STRING");
    ASSERT_NOK_WITH_MSG(LiteralSerDeUtils::GetFixedFieldSize(FieldType::BINARY),
                        "Unsupported field type for GetFixedFieldSize: BINARY");
}

TEST_F(ChunkedDictionaryTest, TestLiteralSerDeUtilsGetSerializedSizeInBytes) {
    ASSERT_EQ(LiteralSerDeUtils::GetSerializedSizeInBytes(Literal(true)).value(), sizeof(bool));
    ASSERT_EQ(LiteralSerDeUtils::GetSerializedSizeInBytes(Literal(42)).value(), sizeof(int32_t));
    ASSERT_EQ(LiteralSerDeUtils::GetSerializedSizeInBytes(Literal(42.0f)).value(), sizeof(float));
    ASSERT_EQ(
        LiteralSerDeUtils::GetSerializedSizeInBytes(Literal(FieldType::STRING, "abc", 3)).value(),
        7);
    ASSERT_NOK_WITH_MSG(LiteralSerDeUtils::GetSerializedSizeInBytes(Literal(FieldType::BINARY)),
                        "Unsupported field type for GetSerializedSizeInBytes: BINARY");
}

TEST_F(ChunkedDictionaryTest, TestLiteralSerDeUtilsUnsupportedCreateValueWriter) {
    ASSERT_NOK_WITH_MSG(LiteralSerDeUtils::CreateValueWriter(FieldType::BINARY),
                        "Unsupported field type for literal serialization: BINARY");
}

TEST_F(ChunkedDictionaryTest, TestLiteralSerDeUtilsUnsupportedCreateValueReader) {
    ASSERT_NOK_WITH_MSG(LiteralSerDeUtils::CreateValueReader(FieldType::BINARY),
                        "Unsupported field type for literal deserialization: BINARY");
}

TEST_F(ChunkedDictionaryTest, TestStringLiteralSerde) {
    ASSERT_OK_AND_ASSIGN(auto serializer, LiteralSerDeUtils::CreateValueWriter(FieldType::STRING));
    ASSERT_OK_AND_ASSIGN(auto deserializer,
                         LiteralSerDeUtils::CreateValueReader(FieldType::STRING));
    auto data_out = std::make_shared<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    ASSERT_OK(serializer(data_out, Literal(FieldType::STRING, "abc", 3)));
    auto bytes = MemorySegmentUtils::CopyToBytes(
        data_out->Segments(), 0, static_cast<int32_t>(data_out->CurrentSize()), pool_.get());
    auto data_in = std::make_shared<DataInputStream>(
        std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size()));
    ASSERT_OK_AND_ASSIGN(Literal literal, deserializer(data_in, pool_.get()));
    ASSERT_EQ(literal.GetValue<std::string>(), "abc");
}

// Parameterized test: multiple chunk size_limit and cardinality
class ChunkedDictionaryParamTest : public ::testing::TestWithParam<std::tuple<int32_t, int32_t>> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        pool_.reset();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;

    // Template helper function to test floating-point types (float or double)
    template <typename T>
    void TestFloatingPointSizeLimitAndCardinality(FieldType field_type, int32_t chunk_size_limit,
                                                  int32_t cardinality) {
        constexpr T kMagicNonExisting = static_cast<T>(254514.2071);
        constexpr T kBound = static_cast<T>(10000.0);

        ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(field_type));
        ASSERT_OK_AND_ASSIGN(auto appender, ChunkedDictionary::Appender::Create(
                                                key_factory, chunk_size_limit, pool_));

        // Build random, unique, sorted keys
        // Ensure that the magic non-existing value is never generated.
        std::mt19937 rng(0x12345678u ^
                         static_cast<uint32_t>(chunk_size_limit * 131 + cardinality * 17));
        std::uniform_real_distribution<T> dist(-kBound, kBound);

        std::unordered_set<T> used;
        std::vector<T> keys;
        keys.reserve(static_cast<size_t>(cardinality));
        while (static_cast<int32_t>(keys.size()) < cardinality) {
            T v = dist(rng);
            if (v == kMagicNonExisting) {
                continue;
            }
            if (used.insert(v).second) {
                keys.push_back(v);
            }
        }
        std::sort(keys.begin(), keys.end());

        for (int32_t i = 0; i < cardinality; ++i) {
            ASSERT_OK(appender->AppendSorted(Literal(keys[static_cast<size_t>(i)]), i));
        }

        ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
        auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
        ASSERT_OK_AND_ASSIGN(auto dict,
                             ChunkedDictionary::Create(field_type, input_stream, 0, pool_));

        // Find code by key and find key by code
        for (int32_t i = 0; i < cardinality; ++i) {
            const T key = keys[static_cast<size_t>(i)];
            ASSERT_OK_AND_ASSIGN(int32_t code, dict->Find(Literal(key)));
            ASSERT_EQ(code, i);

            ASSERT_OK_AND_ASSIGN(Literal literal, dict->Find(i));
            ASSERT_EQ(literal, Literal(key));
        }

        // Find by code out of range (invalid)
        ASSERT_NOK_WITH_MSG(dict->Find(-1), "Invalid code: -1");
        ASSERT_NOK_WITH_MSG(dict->Find(cardinality),
                            fmt::format("Invalid: Invalid Code: {}", cardinality));

        ASSERT_TRUE(used.find(kMagicNonExisting) == used.end());
        ASSERT_OK_AND_ASSIGN(int32_t code, dict->Find(Literal(kMagicNonExisting)));
        ASSERT_LT(code, 0);
    }
};

TEST_P(ChunkedDictionaryParamTest, TestSizeLimitAndCardinalityFloat) {
    const int32_t chunk_size_limit = std::get<0>(GetParam());
    const int32_t cardinality = std::get<1>(GetParam());
    TestFloatingPointSizeLimitAndCardinality<float>(FieldType::FLOAT, chunk_size_limit,
                                                    cardinality);
}

TEST_P(ChunkedDictionaryParamTest, TestSizeLimitAndCardinalityDouble) {
    const int32_t chunk_size_limit = std::get<0>(GetParam());
    const int32_t cardinality = std::get<1>(GetParam());
    TestFloatingPointSizeLimitAndCardinality<double>(FieldType::DOUBLE, chunk_size_limit,
                                                     cardinality);
}

TEST_F(ChunkedDictionaryTest, TestChunkFindFirstKeyFastPath) {
    ASSERT_OK_AND_ASSIGN(auto key_factory, KeyFactory::Create(FieldType::INT));
    // Large chunk size so all keys land in one chunk
    ASSERT_OK_AND_ASSIGN(auto appender,
                         ChunkedDictionary::Appender::Create(key_factory, 1024 * 1024, pool_));

    int32_t key_count = 10;
    for (int32_t i = 0; i < key_count; i++) {
        ASSERT_OK(appender->AppendSorted(Literal((i + 1) * 10), i));
    }

    ASSERT_OK_AND_ASSIGN(auto bytes, appender->Serialize());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto dict,
                         ChunkedDictionary::Create(FieldType::INT, input_stream, 0, pool_));

    {
        ASSERT_OK_AND_ASSIGN(int32_t code, dict->Find(Literal(10)));
        ASSERT_EQ(code, 0);
    }
    {
        ASSERT_OK_AND_ASSIGN(Literal literal, dict->Find(0));
        ASSERT_EQ(literal, Literal(10));
    }
    {
        ASSERT_OK_AND_ASSIGN(int32_t code, dict->Find(Literal(50)));
        ASSERT_EQ(code, 4);

        ASSERT_OK_AND_ASSIGN(Literal literal, dict->Find(4));
        ASSERT_EQ(literal, Literal(50));
    }
    {
        ASSERT_OK_AND_ASSIGN(int32_t code, dict->Find(Literal(15)));
        ASSERT_LT(code, 0);
    }
}

INSTANTIATE_TEST_SUITE_P(
    SizeLimitAndCardinality, ChunkedDictionaryParamTest,
    ::testing::Combine(::testing::Values(1, 16, 64, 128, 1024),               // chunk size limit
                       ::testing::Values(1, 5, 20, 100, 666, 8888, 22222)));  // cardinality

}  // namespace paimon::test
