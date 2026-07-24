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
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "fmt/format.h"
#include "paimon/cache/cache.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/format/parquet/parquet_file_batch_reader.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/reader_builder.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/memory/memory_segment.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"

namespace paimon::parquet {

class ParquetReaderBuilder : public ReaderBuilder {
 public:
    ParquetReaderBuilder(const std::map<std::string, std::string>& options, int32_t batch_size)
        : batch_size_(batch_size), pool_(GetDefaultPool()), options_(options) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    ReaderBuilder* WithCache(const std::shared_ptr<Cache>& cache) override {
        cache_ = cache;
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& path) const override {
        try {
            PAIMON_ASSIGN_OR_RAISE(int64_t file_length, path->Length());
            std::string file_uri;
            if (cache_) {
                Result<std::string> file_uri_result = path->GetUri();
                if (file_uri_result.ok()) {
                    file_uri = std::move(file_uri_result).value();
                }
            }
            std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool_);
            auto unique_input_stream =
                std::make_unique<ArrowInputStreamAdapter>(path, file_length, arrow_pool);
            auto storage_read_bytes = unique_input_stream->StorageReadBytes();
            std::shared_ptr<arrow::io::RandomAccessFile> input_stream(
                std::move(unique_input_stream));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<::parquet::FileMetaData> file_metadata,
                                   GetCachedParquetMetadata(input_stream, file_uri, arrow_pool));
            return ParquetFileBatchReader::Create(std::move(input_stream), options_, batch_size_,
                                                  std::move(file_metadata),
                                                  std::move(storage_read_bytes), arrow_pool);
        }
        PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("ParquetReaderBuilder::Build")
    }

 private:
    Result<MemorySegment> SerializeParquetMetadataFooter(
        const std::shared_ptr<arrow::io::RandomAccessFile>& input_stream,
        const ::parquet::ReaderProperties& reader_properties,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool) const {
        constexpr int64_t kParquetFooterSize = 8;

        std::shared_ptr<::parquet::FileMetaData> metadata =
            ::parquet::ParquetFileReader::Open(input_stream, reader_properties)->metadata();
        if (metadata == nullptr) {
            return Status::Invalid("Failed to read parquet metadata");
        }

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::io::BufferOutputStream> output_stream,
            arrow::io::BufferOutputStream::Create(metadata->size() + kParquetFooterSize,
                                                  arrow_pool.get()));
        ::parquet::WriteFileMetaData(*metadata, output_stream.get());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> metadata_footer,
                                          output_stream->Finish());

        MemorySegment segment =
            MemorySegment::AllocateHeapMemory(metadata_footer->size(), pool_.get());
        std::memcpy(segment.MutableData(), metadata_footer->data(), metadata_footer->size());
        return segment;
    }

    static Result<std::shared_ptr<::parquet::FileMetaData>> ParseParquetMetadataFooter(
        const MemorySegment& segment, const ::parquet::ReaderProperties& reader_properties) {
        if (segment.Data() == nullptr || segment.Size() <= 0) {
            return Status::Invalid("Parquet metadata cache value is empty");
        }

        auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(segment.Data()), segment.Size());
        auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
        std::shared_ptr<::parquet::FileMetaData> metadata =
            ::parquet::ParquetFileReader::Open(buffer_reader, reader_properties)->metadata();
        if (metadata == nullptr) {
            return Status::Invalid("Failed to parse parquet metadata footer");
        }
        return metadata;
    }

    Result<std::shared_ptr<::parquet::FileMetaData>> GetCachedParquetMetadata(
        const std::shared_ptr<arrow::io::RandomAccessFile>& input_stream,
        const std::string& file_uri, const std::shared_ptr<arrow::MemoryPool>& arrow_pool) const {
        if (!cache_ || file_uri.empty()) {
            return std::shared_ptr<::parquet::FileMetaData>();
        }
        PAIMON_ASSIGN_OR_RAISE(
            ::parquet::ReaderProperties reader_properties,
            ParquetFileBatchReader::CreateReaderProperties(arrow_pool, options_));

        auto cache_key = CacheKey::ForKind(file_uri, /*position=*/-1, /*length=*/-1,
                                           CacheKind::DATA_FILE_FOOTER);
        auto supplier =
            [this, &input_stream, reader_properties,
             arrow_pool](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
            PAIMON_ASSIGN_OR_RAISE(
                MemorySegment segment,
                SerializeParquetMetadataFooter(input_stream, reader_properties, arrow_pool));
            return std::make_shared<CacheValue>(segment, CacheCallback());
        };

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CacheValue> cache_value,
                               cache_->Get(cache_key, supplier));
        if (cache_value == nullptr) {
            return Status::Invalid("Parquet metadata cache returned nullptr value");
        }
        return ParseParquetMetadataFooter(cache_value->GetSegment(), reader_properties);
    }

    int32_t batch_size_ = -1;
    std::shared_ptr<MemoryPool> pool_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<Cache> cache_;
};

}  // namespace paimon::parquet
