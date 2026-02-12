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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "avro/DataFile.hh"
#include "avro/Stream.hh"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/format/avro/avro_format_defs.h"
#include "paimon/format/avro/avro_format_writer.h"
#include "paimon/format/avro/avro_output_stream_impl.h"
#include "paimon/format/writer_builder.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow
namespace paimon {
class FormatWriter;
class OutputStream;
}  // namespace paimon

namespace paimon::avro {

class AvroWriterBuilder : public WriterBuilder {
 public:
    AvroWriterBuilder(const std::shared_ptr<arrow::Schema>& schema, int32_t batch_size,
                      const std::map<std::string, std::string>& options)
        : pool_(GetDefaultPool()), schema_(schema), options_(options) {}

    WriterBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FormatWriter>> Build(const std::shared_ptr<OutputStream>& out,
                                                const std::string& compression) override {
        auto output_stream = std::make_unique<AvroOutputStreamImpl>(out, BUFFER_SIZE, pool_);
        PAIMON_ASSIGN_OR_RAISE(
            std::string file_compression,
            OptionsUtils::GetValueFromMap<std::string>(options_, AVRO_CODEC, compression));
        PAIMON_ASSIGN_OR_RAISE(::avro::Codec codec,
                               ToAvroCompressionKind(StringUtils::ToLowerCase(file_compression)));
        PAIMON_ASSIGN_OR_RAISE(std::optional<int32_t> compression_level,
                               GetAvroCompressionLevel(codec));
        return AvroFormatWriter::Create(std::move(output_stream), schema_, codec,
                                        compression_level);
    }

 private:
    static constexpr int32_t BUFFER_SIZE = 1024 * 1024;

    static Result<::avro::Codec> ToAvroCompressionKind(const std::string& file_compression) {
        if (file_compression == "zstd" || file_compression == "zstandard") {
            return ::avro::Codec::ZSTD_CODEC;
        } else if (file_compression == "snappy") {
            return ::avro::Codec::SNAPPY_CODEC;
        } else if (file_compression == "null") {
            return ::avro::Codec::NULL_CODEC;
        } else if (file_compression == "deflate") {
            return ::avro::Codec::DEFLATE_CODEC;
        } else {
            return Status::Invalid("unknown compression " + file_compression);
        }
    }
    Result<std::optional<int32_t>> GetAvroCompressionLevel(const ::avro::Codec& codec) {
        std::optional<int32_t> compression_level;
        if (codec == ::avro::Codec::ZSTD_CODEC) {
            PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
            compression_level = core_options.GetFileCompressionZstdLevel();
        }
        return compression_level;
    }

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    const std::map<std::string, std::string> options_;
};

}  // namespace paimon::avro
