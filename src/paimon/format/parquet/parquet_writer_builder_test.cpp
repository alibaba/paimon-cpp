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

#include "paimon/format/parquet/parquet_writer_builder.h"

#include <limits>

#include "arrow/type_fwd.h"
#include "arrow/util/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/properties.h"
#include "parquet/type_fwd.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::parquet::test {

TEST(ParquetWriterBuilderTest, DefaultPrepareWriterProperties) {
    arrow::FieldVector fields;
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::map<std::string, std::string> options;
    options[Options::FILE_FORMAT] = "parquet";
    options[Options::MANIFEST_FORMAT] = "parquet";
    ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
    ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("zstd"));
    ASSERT_EQ(::parquet::ParquetVersion::PARQUET_2_6, properties->version());
    ASSERT_EQ(1024 * 1024, properties->dictionary_pagesize_limit());
    ASSERT_EQ(1024 * 1024, properties->data_pagesize());
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), properties->max_row_group_length());
    ASSERT_EQ(128 * 1024 * 1024, properties->max_row_group_size());
    ASSERT_EQ(1024, properties->write_batch_size());
    ASSERT_EQ(1, properties->default_column_properties().compression_level());
    ASSERT_TRUE(properties->store_decimal_as_integer());
}

TEST(ParquetWriterBuilderTest, PrepareWriterProperties) {
    arrow::FieldVector fields;
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::map<std::string, std::string> options;
    options[PARQUET_PAGE_SIZE] = "1024";
    options[PARQUET_DICTIONARY_PAGE_SIZE] = "4096";
    options[PARQUET_WRITER_VERSION] = "PARQUET_2_0";
    options[PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL] = "3";
    options[PARQUET_BLOCK_SIZE] = "2048";
    options[Options::FILE_FORMAT] = "parquet";
    options[Options::MANIFEST_FORMAT] = "parquet";
    ParquetWriterBuilder builder(schema, /*batch_size=*/1024 * 1024, options);
    ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("zstd"));
    // Version numbering differs between C++ and Java Parquet implementations. Java's PARQUET_2_0
    // format version corresponds to C++'s PARQUET_2_6 enum value.
    ASSERT_EQ(::parquet::ParquetVersion::PARQUET_2_6, properties->version());
    ASSERT_EQ(4096, properties->dictionary_pagesize_limit());
    ASSERT_EQ(1024, properties->data_pagesize());
    ASSERT_EQ(2048, properties->max_row_group_size());
    ASSERT_EQ(1024 * 1024, properties->write_batch_size());
    ASSERT_EQ(3, properties->default_column_properties().compression_level());
}

TEST(ParquetWriterBuilderTest, PrepareWriterPropertiesWithZstdLevelPriority) {
    arrow::FieldVector fields;
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_COMPRESSION_ZSTD_LEVEL] = "4";
        options[PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL] = "3";
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024 * 1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("zstd"));
        ASSERT_EQ(3, properties->default_column_properties().compression_level());
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_COMPRESSION_ZSTD_LEVEL] = "4";
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024 * 1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("zstd"));
        ASSERT_EQ(4, properties->default_column_properties().compression_level());
    }
}

TEST(ParquetWriterBuilderTest, TestPrepareWriterPropertiesFileCompression) {
    arrow::FieldVector fields;
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("lz4"));
        ASSERT_EQ(properties->default_column_properties().compression(),
                  arrow::Compression::LZ4_FRAME);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("lz4_raW"));
        ASSERT_EQ(properties->default_column_properties().compression(), arrow::Compression::LZ4);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("zstd"));
        ASSERT_EQ(properties->default_column_properties().compression(), arrow::Compression::ZSTD);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("LZ4"));
        ASSERT_EQ(properties->default_column_properties().compression(),
                  arrow::Compression::LZ4_FRAME);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("ZSTd"));
        ASSERT_EQ(properties->default_column_properties().compression(), arrow::Compression::ZSTD);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        options[PARQUET_COMPRESSION_CODEC_ZLIB_LEVEL] = "3";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("gzip"));
        ASSERT_EQ(3, properties->default_column_properties().compression_level());
        ASSERT_EQ(properties->default_column_properties().compression(), arrow::Compression::GZIP);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("snappy"));
        ASSERT_EQ(properties->default_column_properties().compression(),
                  arrow::Compression::SNAPPY);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("lzo"));
        ASSERT_EQ(properties->default_column_properties().compression(), arrow::Compression::LZO);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_NOK(builder.PrepareWriterProperties("unknown"));
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("uncompressed"));
        ASSERT_EQ(properties->default_column_properties().compression(),
                  arrow::Compression::UNCOMPRESSED);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("lz4_hadoop"));
        ASSERT_EQ(properties->default_column_properties().compression(),
                  arrow::Compression::LZ4_HADOOP);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        options[PARQUET_COMPRESSION_CODEC_BROTLI_LEVEL] = "2";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("brotli"));
        ASSERT_EQ(2, properties->default_column_properties().compression_level());
        ASSERT_EQ(properties->default_column_properties().compression(),
                  arrow::Compression::BROTLI);
    }
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_FORMAT] = "parquet";
        options[Options::MANIFEST_FORMAT] = "parquet";
        ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
        ASSERT_OK_AND_ASSIGN(auto properties, builder.PrepareWriterProperties("None"));
        ASSERT_EQ(properties->default_column_properties().compression(),
                  arrow::Compression::UNCOMPRESSED);
    }
}

TEST(ParquetWriterBuilderTest, TestInvalidWriterVersion) {
    arrow::FieldVector fields;
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::map<std::string, std::string> options;
    options[Options::FILE_FORMAT] = "parquet";
    options[Options::MANIFEST_FORMAT] = "parquet";
    options[PARQUET_WRITER_VERSION] = "PARQUET_3_0";
    ParquetWriterBuilder builder(schema, /*batch_size=*/1024, options);
    ASSERT_NOK_WITH_MSG(builder.PrepareWriterProperties("zstd"),
                        "Unknown writer version PARQUET_3_0");
}

}  // namespace paimon::parquet::test
