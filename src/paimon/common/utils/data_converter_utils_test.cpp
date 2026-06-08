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

#include "paimon/common/utils/data_converter_utils.h"

#include <cstddef>
#include <memory>
#include <vector>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DataConverterUtilsTest, TestDataToBinaryRowConverterWithLegacyPartitionName) {
    auto pool = GetDefaultPool();
    std::vector<std::pair<std::string, arrow::Type::type>> data = {
        {"true", arrow::Type::BOOL},
        {"10", arrow::Type::INT8},
        {"-20", arrow::Type::INT8},
        {"1556", arrow::Type::INT16},
        {"-2556", arrow::Type::INT16},
        {"348489", arrow::Type::INT32},
        {"-448489", arrow::Type::INT32},
        {"279039", arrow::Type::INT64},
        {"1234567", arrow::Type::INT64},
        {"abcde", arrow::Type::STRING},
        {"这是一个很长很长的中文", arrow::Type::STRING},
        {"10440", arrow::Type::DATE32}};

    std::vector<DataConverterUtils::StrToBinaryRowConverter> converters;
    std::vector<DataConverterUtils::BinaryRowFieldToStrConverter> reconverters;
    for (const auto& [value, type] : data) {
        ASSERT_OK_AND_ASSIGN(auto converter,
                             DataConverterUtils::CreateDataToBinaryRowConverter(type, pool.get()));
        converters.emplace_back(std::move(converter));
        ASSERT_OK_AND_ASSIGN(auto reconverter,
                             DataConverterUtils::CreateBinaryRowFieldToStringConverter(
                                 type, /*legacy_partition_name_enabled=*/true));
        reconverters.emplace_back(reconverter);
    }
    // test not implement type
    ASSERT_NOK(DataConverterUtils::CreateDataToBinaryRowConverter(arrow::Type::LIST, pool.get()));

    BinaryRow row(data.size());
    BinaryRowWriter writer(&row, 0, pool.get());
    for (size_t idx = 0; idx < data.size(); idx++) {
        ASSERT_OK(converters[idx](data[idx].first, idx, &writer));
    }
    // test invalid str
    ASSERT_NOK(converters[0]("abc", /*idx=*/0, &writer));
    writer.Complete();

    ASSERT_EQ(data.size(), row.GetFieldCount());
    ASSERT_EQ(true, row.GetBoolean(0));
    ASSERT_EQ(10, row.GetByte(1));
    ASSERT_EQ(-20, row.GetByte(2));
    ASSERT_EQ(1556, row.GetShort(3));
    ASSERT_EQ(-2556, row.GetShort(4));
    ASSERT_EQ(348489, row.GetInt(5));
    ASSERT_EQ(-448489, row.GetInt(6));
    ASSERT_EQ(279039, row.GetLong(7));
    ASSERT_EQ(1234567, row.GetLong(8));
    ASSERT_EQ("abcde", row.GetString(9).ToString());
    ASSERT_EQ("这是一个很长很长的中文", row.GetString(10).ToString());
    ASSERT_EQ(10440, row.GetDate(11));

    for (size_t idx = 0; idx < data.size(); idx++) {
        ASSERT_OK_AND_ASSIGN(auto partition_field_str, reconverters[idx](row, idx));
        ASSERT_EQ(data[idx].first, partition_field_str);
    }
}

TEST(DataConverterUtilsTest, TestDataToBinaryRowConverterWithNoLegacyPartitionName) {
    auto pool = GetDefaultPool();
    std::vector<std::pair<std::string, arrow::Type::type>> data = {
        {"true", arrow::Type::BOOL},
        {"10", arrow::Type::INT8},
        {"-20", arrow::Type::INT8},
        {"1556", arrow::Type::INT16},
        {"-2556", arrow::Type::INT16},
        {"348489", arrow::Type::INT32},
        {"-448489", arrow::Type::INT32},
        {"279039", arrow::Type::INT64},
        {"1234567", arrow::Type::INT64},
        {"abcde", arrow::Type::STRING},
        {"这是一个很长很长的中文", arrow::Type::STRING},
        {"1998-08-02", arrow::Type::DATE32}};

    std::vector<DataConverterUtils::StrToBinaryRowConverter> converters;
    std::vector<DataConverterUtils::BinaryRowFieldToStrConverter> reconverters;
    for (const auto& [value, type] : data) {
        ASSERT_OK_AND_ASSIGN(auto converter,
                             DataConverterUtils::CreateDataToBinaryRowConverter(type, pool.get()));
        converters.emplace_back(std::move(converter));
        ASSERT_OK_AND_ASSIGN(auto reconverter,
                             DataConverterUtils::CreateBinaryRowFieldToStringConverter(
                                 type, /*legacy_partition_name_enabled=*/false));
        reconverters.emplace_back(reconverter);
    }
    BinaryRow row(data.size());
    BinaryRowWriter writer(&row, 0, pool.get());
    for (size_t idx = 0; idx < data.size(); idx++) {
        ASSERT_OK(converters[idx](data[idx].first, idx, &writer));
    }
    writer.Complete();

    ASSERT_EQ(data.size(), row.GetFieldCount());
    ASSERT_EQ(true, row.GetBoolean(0));
    ASSERT_EQ(10, row.GetByte(1));
    ASSERT_EQ(-20, row.GetByte(2));
    ASSERT_EQ(1556, row.GetShort(3));
    ASSERT_EQ(-2556, row.GetShort(4));
    ASSERT_EQ(348489, row.GetInt(5));
    ASSERT_EQ(-448489, row.GetInt(6));
    ASSERT_EQ(279039, row.GetLong(7));
    ASSERT_EQ(1234567, row.GetLong(8));
    ASSERT_EQ("abcde", row.GetString(9).ToString());
    ASSERT_EQ("这是一个很长很长的中文", row.GetString(10).ToString());
    ASSERT_EQ(10440, row.GetDate(11));

    for (size_t idx = 0; idx < data.size(); idx++) {
        ASSERT_OK_AND_ASSIGN(auto partition_field_str, reconverters[idx](row, idx));
        ASSERT_EQ(data[idx].first, partition_field_str);
    }
}

}  // namespace paimon::test
