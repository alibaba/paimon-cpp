/*
 * Copyright 2025-present Alibaba Inc.
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

#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/table/source/fallback_data_split.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/global_index/indexed_split.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
namespace paimon {
struct DataFileMeta;
namespace {
Status WriteDataSplit(const std::shared_ptr<DataSplitImpl>& data_split_impl,
                      MemorySegmentOutputStream* out, const std::shared_ptr<MemoryPool>& pool) {
    out->WriteValue<int64_t>(DataSplitImpl::MAGIC);
    out->WriteValue<int32_t>(DataSplitImpl::VERSION);
    out->WriteValue<int64_t>(data_split_impl->SnapshotId());

    PAIMON_RETURN_NOT_OK(SerializationUtils::SerializeBinaryRow(data_split_impl->Partition(), out));
    out->WriteValue<int32_t>(data_split_impl->Bucket());
    out->WriteString(data_split_impl->BucketPath());

    std::optional<int32_t> total_buckets = data_split_impl->TotalBuckets();
    if (total_buckets == std::nullopt) {
        out->WriteValue<bool>(false);
    } else {
        out->WriteValue<bool>(true);
        out->WriteValue<int32_t>(total_buckets.value());
    }

    DataFileMetaSerializer serializer(pool);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(data_split_impl->BeforeFiles(), out));

    DeletionFile::SerializeList(data_split_impl->BeforeDeletionFiles(), out);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(data_split_impl->DataFiles(), out));
    DeletionFile::SerializeList(data_split_impl->DeletionFiles(), out);
    out->WriteValue<bool>(data_split_impl->IsStreaming());
    out->WriteValue<bool>(data_split_impl->RawConvertible());
    return Status::OK();
}

Result<std::shared_ptr<DataSplitImpl>> ReadDataSplitWithoutMagicNumber(
    int64_t magic, DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    int32_t version = 1;
    if (magic == DataSplitImpl::MAGIC) {
        PAIMON_ASSIGN_OR_RAISE(version, in->ReadValue<int32_t>());
    }

    // version 1 does not write magic number in, so the first long is snapshot id.
    int64_t snapshot_id = magic;
    if (version != 1) {
        PAIMON_ASSIGN_OR_RAISE(snapshot_id, in->ReadValue<int64_t>());
    }

    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                           SerializationUtils::DeserializeBinaryRow(in, pool.get()));
    int32_t bucket = -1;
    PAIMON_ASSIGN_OR_RAISE(bucket, in->ReadValue<int32_t>());
    std::string bucket_path;
    PAIMON_ASSIGN_OR_RAISE(bucket_path, in->ReadString());

    std::optional<int32_t> total_buckets;
    if (version >= 6) {
        PAIMON_ASSIGN_OR_RAISE(bool total_buckets_exist, in->ReadValue<bool>());
        if (total_buckets_exist) {
            PAIMON_ASSIGN_OR_RAISE(total_buckets, in->ReadValue<int32_t>());
        }
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ObjectSerializer<std::shared_ptr<DataFileMeta>>> data_file_serializer,
        DataSplitImpl::GetFileMetaSerializer(version, pool));
    std::vector<std::shared_ptr<DataFileMeta>> before_files;
    PAIMON_ASSIGN_OR_RAISE(before_files, data_file_serializer->DeserializeList(in));
    // compatible for deletion file
    std::vector<std::optional<DeletionFile>> before_deletion_files;
    PAIMON_ASSIGN_OR_RAISE(before_deletion_files, DeletionFile::DeserializeList(in, version));

    std::vector<std::shared_ptr<DataFileMeta>> data_files;
    PAIMON_ASSIGN_OR_RAISE(data_files, data_file_serializer->DeserializeList(in));
    // compatible for deletion file
    std::vector<std::optional<DeletionFile>> data_deletion_files;
    PAIMON_ASSIGN_OR_RAISE(data_deletion_files, DeletionFile::DeserializeList(in, version));

    bool is_streaming = false;
    PAIMON_ASSIGN_OR_RAISE(is_streaming, in->ReadValue<bool>());
    bool raw_convertible = false;
    PAIMON_ASSIGN_OR_RAISE(raw_convertible, in->ReadValue<bool>());

    DataSplitImpl::Builder builder(partition, bucket, bucket_path, std::move(data_files));
    builder.WithTotalBuckets(total_buckets)
        .WithSnapshot(snapshot_id)
        .WithBeforeFiles(std::move(before_files))
        .IsStreaming(is_streaming)
        .RawConvertible(raw_convertible);
    if (!before_deletion_files.empty()) {
        builder.WithBeforeDeletionFiles(before_deletion_files);
    }
    if (!data_deletion_files.empty()) {
        builder.WithDataDeletionFiles(data_deletion_files);
    }
    return builder.Build();
}

}  // namespace

Result<std::string> Split::Serialize(const std::shared_ptr<Split>& split,
                                     const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    if (auto data_split_impl = std::dynamic_pointer_cast<DataSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(WriteDataSplit(data_split_impl, &out, pool));
    } else if (auto indexed_split_impl = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        out.WriteValue<int64_t>(IndexedSplitImpl::MAGIC);
        out.WriteValue<int32_t>(IndexedSplitImpl::VERSION);
        auto inner_split_impl =
            std::dynamic_pointer_cast<DataSplitImpl>(indexed_split_impl->GetDataSplit());
        if (!inner_split_impl) {
            return Status::Invalid("inner split in IndexedSplit is supposed to be DataSplit");
        }
        PAIMON_RETURN_NOT_OK(WriteDataSplit(inner_split_impl, &out, pool));
        auto row_ranges = indexed_split_impl->RowRanges();
        out.WriteValue<int32_t>(row_ranges.size());
        for (const auto& range : row_ranges) {
            out.WriteValue<int64_t>(range.from);
            out.WriteValue<int64_t>(range.to);
        }

        auto scores = indexed_split_impl->Scores();
        if (!scores.empty()) {
            out.WriteValue<bool>(true);
            out.WriteValue<int32_t>(scores.size());
            for (const auto& score : scores) {
                out.WriteValue<float>(score);
            }
        } else {
            out.WriteValue<bool>(false);
        }
    } else {
        return Status::Invalid("invalid split, cannot cast to DataSplit or IndexedSplit");
    }
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

Result<std::shared_ptr<Split>> Split::Deserialize(const char* buffer, size_t length,
                                                  const std::shared_ptr<MemoryPool>& pool) {
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, length);
    DataInputStream in(input_stream);

    int64_t magic = -1;
    PAIMON_ASSIGN_OR_RAISE(magic, in.ReadValue<int64_t>());

    if (magic == IndexedSplitImpl::MAGIC) {
        PAIMON_ASSIGN_OR_RAISE(int32_t version, in.ReadValue<int32_t>());
        if (version != IndexedSplitImpl::VERSION) {
            return Status::Invalid(fmt::format("Unsupported IndexedSplit version: {}", version));
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t data_split_magic, in.ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> data_split,
                               ReadDataSplitWithoutMagicNumber(data_split_magic, &in, pool));
        PAIMON_ASSIGN_OR_RAISE(int32_t range_size, in.ReadValue<int32_t>());
        std::vector<Range> row_ranges;
        row_ranges.reserve(range_size);
        for (int32_t i = 0; i < range_size; ++i) {
            PAIMON_ASSIGN_OR_RAISE(int64_t range_from, in.ReadValue<int64_t>());
            PAIMON_ASSIGN_OR_RAISE(int64_t range_to, in.ReadValue<int64_t>());
            row_ranges.emplace_back(range_from, range_to);
        }
        std::vector<float> scores;
        PAIMON_ASSIGN_OR_RAISE(bool has_scores, in.ReadValue<bool>());
        if (has_scores) {
            PAIMON_ASSIGN_OR_RAISE(int32_t scores_length, in.ReadValue<int32_t>());
            scores.resize(scores_length);
            for (int32_t i = 0; i < scores_length; ++i) {
                PAIMON_ASSIGN_OR_RAISE(float score, in.ReadValue<float>());
                scores[i] = score;
            }
        }
        // TODO(lisizhuo.lsz): support fallback split in IndexedSplit
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
        PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
        if (pos == stream_length) {
            return std::make_shared<IndexedSplitImpl>(data_split, row_ranges, scores);
        } else if (pos == stream_length - 1) {
            return Status::Invalid(
                "invalid IndexedSplit, do not support FallbackSplit in IndexedSplit");
        } else {
            return Status::Invalid(
                fmt::format("invalid IndexedSplit, remaining {} bytes after deserializing",
                            stream_length - pos));
        }
    } else if (magic == DataSplitImpl::MAGIC) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> data_split,
                               ReadDataSplitWithoutMagicNumber(magic, &in, pool));
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
        PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
        if (pos == stream_length) {
            return data_split;
        } else if (pos == stream_length - 1) {
            PAIMON_ASSIGN_OR_RAISE(bool is_fallback, in.ReadValue<bool>());
            return std::make_shared<FallbackDataSplit>(data_split, is_fallback);
        } else {
            return Status::Invalid(fmt::format(
                "invalid data split byte stream, remaining {} bytes after deserializing",
                stream_length - pos));
        }
    }
    return Status::Invalid("invalid split, must be DataSplit or IndexedSplit");
}
}  // namespace paimon
