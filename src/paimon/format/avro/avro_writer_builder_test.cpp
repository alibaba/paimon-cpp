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

#include "paimon/format/avro/avro_writer_builder.h"

#include "avro/DataFile.hh"
#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

TEST(AvroWriterBuilderTest, HandlesValidCompressions) {
    ASSERT_OK_AND_ASSIGN(::avro::Codec zstd_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("zstd"));
    ASSERT_EQ(zstd_codec, ::avro::Codec::ZSTD_CODEC);

    ASSERT_OK_AND_ASSIGN(::avro::Codec zstandard_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("zstandard"));
    ASSERT_EQ(zstandard_codec, ::avro::Codec::ZSTD_CODEC);

    ASSERT_OK_AND_ASSIGN(::avro::Codec snappy_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("snappy"));
    ASSERT_EQ(snappy_codec, ::avro::Codec::SNAPPY_CODEC);

    ASSERT_OK_AND_ASSIGN(::avro::Codec null_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("null"));
    ASSERT_EQ(null_codec, ::avro::Codec::NULL_CODEC);

    ASSERT_OK_AND_ASSIGN(::avro::Codec deflate_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("deflate"));
    ASSERT_EQ(deflate_codec, ::avro::Codec::DEFLATE_CODEC);
}

TEST(AvroWriterBuilderTest, HandlesInvalidCompression) {
    ASSERT_NOK(AvroWriterBuilder::ToAvroCompressionKind("unknown_compression"));
}

TEST(AvroWriterBuilderTest, HandlesEmptyString) {
    ASSERT_NOK(AvroWriterBuilder::ToAvroCompressionKind(""));
}

TEST(AvroWriterBuilderTest, CheckAvroCodec) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32())};
    auto schema = std::make_shared<arrow::Schema>(fields);
    {
        AvroWriterBuilder builder(schema, -1,
                                  {{Options::FILE_FORMAT, "avro"}, {"avro.codec", "snappy"}});
        ASSERT_OK_AND_ASSIGN(auto file_writer, builder.Build(nullptr, "zstd"));
        auto* avro_file_writer = dynamic_cast<AvroFormatWriter*>(file_writer.get());
        ASSERT_EQ(avro_file_writer->writer_->codec_, ::avro::Codec::SNAPPY_CODEC);
        ASSERT_EQ(avro_file_writer->writer_->compressionLevel_, std::nullopt);
    }
    {
        AvroWriterBuilder builder(schema, -1,
                                  {{Options::FILE_FORMAT, "avro"}, {"avro.codec", "deflate"}});
        ASSERT_OK_AND_ASSIGN(auto file_writer, builder.Build(nullptr, "zstd"));
        auto* avro_file_writer = dynamic_cast<AvroFormatWriter*>(file_writer.get());
        ASSERT_EQ(avro_file_writer->writer_->codec_, ::avro::Codec::DEFLATE_CODEC);
        ASSERT_EQ(avro_file_writer->writer_->compressionLevel_, std::nullopt);
    }
    {
        AvroWriterBuilder builder(schema, -1,
                                  {{Options::FILE_FORMAT, "avro"}, {"avro.codec", "zstd"}});
        ASSERT_OK_AND_ASSIGN(auto file_writer, builder.Build(nullptr, "zstd"));
        auto* avro_file_writer = dynamic_cast<AvroFormatWriter*>(file_writer.get());
        ASSERT_EQ(avro_file_writer->writer_->codec_, ::avro::Codec::ZSTD_CODEC);
        ASSERT_EQ(avro_file_writer->writer_->compressionLevel_, 1);
    }
    {
        AvroWriterBuilder builder(schema, -1,
                                  {{Options::FILE_FORMAT, "avro"},
                                   {"avro.codec", "zstd"},
                                   {Options::FILE_COMPRESSION_ZSTD_LEVEL, "3"}});
        ASSERT_OK_AND_ASSIGN(auto file_writer, builder.Build(nullptr, "zstd"));
        auto* avro_file_writer = dynamic_cast<AvroFormatWriter*>(file_writer.get());
        ASSERT_EQ(avro_file_writer->writer_->codec_, ::avro::Codec::ZSTD_CODEC);
        ASSERT_EQ(avro_file_writer->writer_->compressionLevel_, 3);
    }
    {
        AvroWriterBuilder builder(schema, -1,
                                  {{Options::FILE_FORMAT, "avro"},
                                   {"avro.codec", "null"},
                                   {Options::FILE_COMPRESSION_ZSTD_LEVEL, "3"}});
        ASSERT_OK_AND_ASSIGN(auto file_writer, builder.Build(nullptr, "zstd"));
        auto* avro_file_writer = dynamic_cast<AvroFormatWriter*>(file_writer.get());
        ASSERT_EQ(avro_file_writer->writer_->codec_, ::avro::Codec::NULL_CODEC);
        ASSERT_EQ(avro_file_writer->writer_->compressionLevel_, std::nullopt);
    }
    {
        AvroWriterBuilder builder(schema, -1,
                                  {{Options::FILE_FORMAT, "avro"},
                                   {"avro.codec", "test"},
                                   {Options::FILE_COMPRESSION_ZSTD_LEVEL, "3"}});
        ASSERT_NOK(builder.Build(nullptr, "zstd"));
    }
}

TEST(AvroWriterBuilderTest, CheckAvroCompressionLevel) {
    {
        AvroWriterBuilder builder(nullptr, -1, {{Options::FILE_FORMAT, "avro"}});
        ASSERT_OK_AND_ASSIGN(std::optional<int32_t> zstd_level,
                             builder.GetAvroCompressionLevel(::avro::Codec::ZSTD_CODEC));
        ASSERT_TRUE(zstd_level.has_value());
        ASSERT_EQ(zstd_level.value(), 1);
    }
    {
        AvroWriterBuilder builder(nullptr, -1, {{Options::FILE_FORMAT, "avro"}});
        ASSERT_OK_AND_ASSIGN(std::optional<int32_t> compression_level,
                             builder.GetAvroCompressionLevel(::avro::Codec::SNAPPY_CODEC));
        ASSERT_FALSE(compression_level.has_value());
    }
    {
        AvroWriterBuilder builder(nullptr, -1, {{Options::FILE_FORMAT, "avro"}});
        ASSERT_OK_AND_ASSIGN(std::optional<int32_t> compression_level,
                             builder.GetAvroCompressionLevel(::avro::Codec::DEFLATE_CODEC));
        ASSERT_FALSE(compression_level.has_value());
    }
    {
        AvroWriterBuilder builder(nullptr, -1, {{Options::FILE_FORMAT, "avro"}});
        ASSERT_OK_AND_ASSIGN(std::optional<int32_t> compression_level,
                             builder.GetAvroCompressionLevel(::avro::Codec::NULL_CODEC));
        ASSERT_FALSE(compression_level.has_value());
    }
    {
        AvroWriterBuilder builder(
            nullptr, -1,
            {{Options::FILE_FORMAT, "avro"}, {Options::FILE_COMPRESSION_ZSTD_LEVEL, "3"}});
        ASSERT_OK_AND_ASSIGN(std::optional<int32_t> zstd_level,
                             builder.GetAvroCompressionLevel(::avro::Codec::ZSTD_CODEC));
        ASSERT_TRUE(zstd_level.has_value());
        ASSERT_EQ(zstd_level.value(), 3);
    }
}

}  // namespace paimon::avro::test
