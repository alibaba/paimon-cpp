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

#include <optional>
#include <utility>

#include "arrow/util/compression.h"
#include "arrow/util/type_fwd.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/status.h"
#include "parquet/properties.h"
#include "parquet/type_fwd.h"

namespace paimon {
class OutputStream;
}  // namespace paimon

namespace paimon::parquet {

Result<std::unique_ptr<FormatWriter>> ParquetWriterBuilder::Build(
    const std::shared_ptr<OutputStream>& out, const std::string& compression) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<::parquet::WriterProperties> writer_properties,
                           PrepareWriterProperties(compression));
    PAIMON_ASSIGN_OR_RAISE(uint64_t max_memory_use, OptionsUtils::GetValueFromMap<uint64_t>(
                                                        options_, PARQUET_WRITER_MAX_MEMORY_USE,
                                                        DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE));

    return ParquetFormatWriter::Create(out, schema_, writer_properties, max_memory_use, pool_);
}

Result<std::shared_ptr<::parquet::WriterProperties>> ParquetWriterBuilder::PrepareWriterProperties(
    const std::string& compression) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Compression::type compression_type,
        arrow::util::Codec::GetCompressionType(StringUtils::ToLowerCase(compression)));
    ::parquet::WriterProperties::Builder builder;
    builder.memory_pool(pool_.get());
    builder.write_batch_size(batch_size_);
    builder.compression(compression_type);
    PAIMON_ASSIGN_OR_RAISE(
        int64_t row_group_length,
        OptionsUtils::GetValueFromMap<int64_t>(options_, PARQUET_WRITE_MAX_ROW_GROUP_LENGTH,
                                               DEFAULT_PARQUET_WRITE_MAX_ROW_GROUP_LENGTH));
    builder.max_row_group_length(row_group_length);
    builder.enable_store_decimal_as_integer();
    if (arrow::util::Codec::SupportsCompressionLevel(compression_type)) {
        std::string key = CompressLevelOptionsKey(compression_type);
        PAIMON_ASSIGN_OR_RAISE(
            int64_t file_compression_level,
            OptionsUtils::GetValueFromMap<int64_t>(options_, key,
                                                   (key == PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL)
                                                       ? core_options.GetFileCompressionZstdLevel()
                                                       : 1));
        builder.compression_level(file_compression_level);
    }

    PAIMON_ASSIGN_OR_RAISE(int64_t row_group_size, OptionsUtils::GetValueFromMap<int64_t>(
                                                       options_, PARQUET_BLOCK_SIZE,
                                                       ::parquet::DEFAULT_MAX_ROW_GROUP_SIZE));
    builder.max_row_group_size(row_group_size);

    PAIMON_ASSIGN_OR_RAISE(int64_t page_size,
                           OptionsUtils::GetValueFromMap<int64_t>(options_, PARQUET_PAGE_SIZE,
                                                                  ::parquet::kDefaultDataPageSize));
    builder.data_pagesize(page_size);
    PAIMON_ASSIGN_OR_RAISE(bool enable_dictionary, OptionsUtils::GetValueFromMap<bool>(
                                                       options_, PARQUET_ENABLE_DICTIONARY,
                                                       ::parquet::DEFAULT_IS_DICTIONARY_ENABLED));
    enable_dictionary ? builder.enable_dictionary() : builder.disable_dictionary();
    PAIMON_ASSIGN_OR_RAISE(
        int64_t dictionary_page_size,
        OptionsUtils::GetValueFromMap<int64_t>(options_, PARQUET_DICTIONARY_PAGE_SIZE,
                                               ::parquet::DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT));
    builder.dictionary_pagesize_limit(dictionary_page_size);
    PAIMON_ASSIGN_OR_RAISE(std::string writer_version,
                           OptionsUtils::GetValueFromMap<std::string>(
                               options_, PARQUET_WRITER_VERSION, std::string("PARQUET_2_0")));
    PAIMON_ASSIGN_OR_RAISE(::parquet::ParquetVersion::type version,
                           ConvertWriterVersion(writer_version));
    builder.version(version);
    return builder.build();
}

Result<::parquet::ParquetVersion::type> ParquetWriterBuilder::ConvertWriterVersion(
    const std::string& writer_version) {
    if (writer_version == "PARQUET_1_0" || writer_version == "v1") {
        return ::parquet::ParquetVersion::type::PARQUET_1_0;
    } else if (writer_version == "PARQUET_2_0" || writer_version == "v2") {
        return ::parquet::ParquetVersion::type::PARQUET_2_6;
    }
    return Status::Invalid(fmt::format("Unknown writer version {}", writer_version));
}

std::string ParquetWriterBuilder::CompressLevelOptionsKey(::arrow::Compression::type compression) {
    switch (compression) {
        case ::arrow::Compression::GZIP:
            return PARQUET_COMPRESSION_CODEC_ZLIB_LEVEL;
        case ::arrow::Compression::BROTLI:
            return PARQUET_COMPRESSION_CODEC_BROTLI_LEVEL;
        case ::arrow::Compression::ZSTD:
            return PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL;
        default:
            return "";
    }
}

}  // namespace paimon::parquet
