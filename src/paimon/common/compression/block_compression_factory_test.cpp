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

#include "paimon/common/compression/block_compression_factory.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::test {
class CompressionFactoryTest : public ::testing::TestWithParam<BlockCompressionType> {};

TEST_P(CompressionFactoryTest, TESTCompressThenDecompress) {
    int32_t original_len = 16;
    BlockCompressionType type = GetParam();

    std::string data(original_len, '\0');
    for (int32_t i = 0; i < original_len; i++) {
        data[i] = static_cast<char>(i);
    }

    ASSERT_OK_AND_ASSIGN(auto factory, BlockCompressionFactory::Create(type));
    ASSERT_EQ(type, factory->GetCompressionType());

    // compress
    auto compressor = factory->GetCompressor();
    auto max_len = compressor->GetMaxCompressedSize(data.size());
    std::string compressed_data(max_len, '\0');
    auto compressed_size =
        compressor->Compress(data.data(), data.size(), compressed_data.data(), max_len);
    ASSERT_OK(compressed_size);
    ASSERT_GT(compressed_size.value(), 0);
    compressed_data.resize(compressed_size.value());

    // decompress
    auto decompressor = factory->GetDecompressor();
    std::string decompressed_data(original_len, '\0');
    auto decompressed_size =
        decompressor->Decompress(compressed_data.data(), compressed_data.size(),
                                 decompressed_data.data(), decompressed_data.size());
    ASSERT_OK(decompressed_size);
    ASSERT_GT(decompressed_size.value(), 0);
    ASSERT_EQ(data, decompressed_data);

    std::string read_write_le{4, '\0'};
    compressor->WriteIntLE(123, read_write_le.data());
    ASSERT_EQ(123, decompressor->ReadIntLE(read_write_le.data()));
    compressor->WriteIntLE(100000, read_write_le.data());
    ASSERT_EQ(100000, decompressor->ReadIntLE(read_write_le.data()));
    compressor->WriteIntLE(-6555, read_write_le.data());
    ASSERT_EQ(-6555, decompressor->ReadIntLE(read_write_le.data()));
    compressor->WriteIntLE(0, read_write_le.data());
    ASSERT_EQ(0, decompressor->ReadIntLE(read_write_le.data()));
}

INSTANTIATE_TEST_SUITE_P(BlockCompressionTypeGroup, CompressionFactoryTest,
                         ::testing::Values(BlockCompressionType::LZ4, BlockCompressionType::ZSTD));

}  // namespace paimon::test
