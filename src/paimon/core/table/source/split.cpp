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

#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/data_file_meta_first_row_id_legacy_serializer.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/table/source/chain_split_impl.h"
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
constexpr int64_t kSplitSerializerMagic = 0x53504C49545F5631L;  // "SPLIT_V1"
constexpr int32_t kSplitSerializerVersion = 1;
constexpr int32_t kSplitSerializerDataSplit = 1;
constexpr int32_t kSplitSerializerIndexedSplit = 3;
constexpr int32_t kSplitSerializerChainSplit = 4;
constexpr int32_t kSplitSerializerFallbackDataSplit = 6;

Result<std::vector<std::shared_ptr<DataFileMeta>>> ReadVersion7DataFileMetaList(
    DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(int32_t size, in->ReadValue<int32_t>());
    if (size < 0) {
        return Status::Invalid(fmt::format("invalid data file meta list size: {}", size));
    }

    DataFileMetaFirstRowIdLegacySerializer legacy_serializer(pool);
    DataFileMetaSerializer current_serializer(pool);
    std::vector<std::shared_ptr<DataFileMeta>> result;
    result.reserve(size);
    for (int32_t i = 0; i < size; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int32_t row_size, in->ReadValue<int32_t>());
        if (row_size < BinaryRow::CalculateFixPartSizeInBytes(19)) {
            return Status::Invalid(
                fmt::format("invalid version 7 data file meta row size: {}", row_size));
        }
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(row_size, pool.get());
        PAIMON_RETURN_NOT_OK(in->ReadBytes(bytes.get()));

        MemorySegment segment = MemorySegment::Wrap(bytes);
        int64_t file_name_offset_and_size =
            segment.GetValue<int64_t>(BinaryRow::CalculateBitSetWidthInBytes(/*arity=*/19));
        if ((file_name_offset_and_size & BinarySection::HIGHEST_FIRST_BIT) != 0) {
            return Status::Invalid(
                "cannot determine version 7 data file meta format from inline file name");
        }
        int32_t variable_part_offset = static_cast<int32_t>(file_name_offset_and_size >> 32);

        int32_t arity = 0;
        const ObjectSerializer<std::shared_ptr<DataFileMeta>>* serializer = nullptr;
        if (variable_part_offset == BinaryRow::CalculateFixPartSizeInBytes(/*arity=*/19)) {
            arity = 19;
            serializer = &legacy_serializer;
        } else if (variable_part_offset == BinaryRow::CalculateFixPartSizeInBytes(/*arity=*/20)) {
            arity = 20;
            serializer = &current_serializer;
        } else {
            return Status::Invalid(fmt::format(
                "invalid version 7 data file meta variable part offset: {}", variable_part_offset));
        }

        BinaryRow row(arity);
        row.PointTo(segment, /*offset=*/0, row_size);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> meta, serializer->FromRow(row));
        result.emplace_back(std::move(meta));
    }
    return result;
}

Result<std::vector<std::shared_ptr<DataFileMeta>>> ReadDataFileMetaList(
    int32_t version, const ObjectSerializer<std::shared_ptr<DataFileMeta>>* data_file_serializer,
    DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    if (version == 7) {
        return ReadVersion7DataFileMetaList(in, pool);
    }
    return data_file_serializer->DeserializeList(in);
}

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
    PAIMON_ASSIGN_OR_RAISE(before_files,
                           ReadDataFileMetaList(version, data_file_serializer.get(), in, pool));
    // compatible for deletion file
    std::vector<std::optional<DeletionFile>> before_deletion_files;
    PAIMON_ASSIGN_OR_RAISE(before_deletion_files, DeletionFile::DeserializeList(in, version));

    std::vector<std::shared_ptr<DataFileMeta>> data_files;
    PAIMON_ASSIGN_OR_RAISE(data_files,
                           ReadDataFileMetaList(version, data_file_serializer.get(), in, pool));
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

Status ValidateFullyRead(const char* split_type, DataInputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, in->GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in->Length());
    if (pos != stream_length) {
        return Status::Invalid(
            fmt::format("invalid {} byte stream, remaining {} bytes after deserializing",
                        split_type, stream_length - pos));
    }
    return Status::OK();
}

Result<std::optional<std::string>> ReadSplitSerializerString(DataInputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(int32_t length, in->ReadValue<int32_t>());
    if (length < 0) {
        return std::optional<std::string>();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, in->GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in->Length());
    if (length > stream_length - pos) {
        return Status::Invalid(
            fmt::format("split serializer string length {} exceeds remaining stream bytes {}",
                        length, stream_length - pos));
    }

    std::string value(length, '\0');
    if (length > 0) {
        PAIMON_RETURN_NOT_OK(in->Read(value.data(), length));
    }
    return std::optional<std::string>(std::move(value));
}

Result<std::unordered_map<std::string, std::string>> ReadSplitSerializerStringMap(
    DataInputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(bool exists, in->ReadValue<bool>());
    if (!exists) {
        return std::unordered_map<std::string, std::string>();
    }

    PAIMON_ASSIGN_OR_RAISE(int32_t size, in->ReadValue<int32_t>());
    if (size < 0) {
        return Status::Invalid(fmt::format("invalid split serializer string map size: {}", size));
    }

    std::unordered_map<std::string, std::string> result;
    result.reserve(size);
    for (int32_t i = 0; i < size; ++i) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> key, ReadSplitSerializerString(in));
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> value, ReadSplitSerializerString(in));
        if (!key || !value) {
            return Status::Invalid("split serializer string map does not support null entries");
        }
        result.insert_or_assign(std::move(key.value()), std::move(value.value()));
    }
    return result;
}

void WriteSplitSerializerString(MemorySegmentOutputStream* out, const std::string& value) {
    out->WriteValue<int32_t>(static_cast<int32_t>(value.size()));
    out->Write(value.data(), static_cast<uint32_t>(value.size()));
}

void WriteSplitSerializerStringMap(MemorySegmentOutputStream* out,
                                   const std::unordered_map<std::string, std::string>& values) {
    out->WriteValue<bool>(true);
    out->WriteValue<int32_t>(static_cast<int32_t>(values.size()));
    for (const auto& [key, value] :
         std::map<std::string, std::string>(values.begin(), values.end())) {
        WriteSplitSerializerString(out, key);
        WriteSplitSerializerString(out, value);
    }
}

Status WriteChainSplitPayload(const std::shared_ptr<ChainSplitImpl>& chain_split,
                              MemorySegmentOutputStream* out,
                              const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_RETURN_NOT_OK(SerializationUtils::SerializeBinaryRow(chain_split->ReadPartition(), out));
    DataFileMetaSerializer serializer(pool);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(chain_split->DataFiles(), out));
    WriteSplitSerializerStringMap(out, chain_split->FileBucketPathMapping());
    WriteSplitSerializerStringMap(out, chain_split->FileBranchMapping());
    return Status::OK();
}

Result<std::shared_ptr<ChainSplitImpl>> ReadChainSplitPayload(
    DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(BinaryRow logical_partition,
                           SerializationUtils::DeserializeBinaryRow(in, pool.get()));

    DataFileMetaSerializer serializer(pool);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> data_files,
                           serializer.DeserializeList(in));
    PAIMON_ASSIGN_OR_RAISE(auto file_bucket_path_mapping, ReadSplitSerializerStringMap(in));
    PAIMON_ASSIGN_OR_RAISE(auto file_branch_mapping, ReadSplitSerializerStringMap(in));
    PAIMON_RETURN_NOT_OK(ValidateFullyRead("ChainSplit", in));

    DataSplitImpl::Builder builder(logical_partition, /*bucket=*/0,
                                   ChainSplitImpl::VIRTUAL_BUCKET_PATH, std::move(data_files));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> base_split, builder.Build());
    return std::make_shared<ChainSplitImpl>(base_split, /*all_snapshot_split=*/false,
                                            logical_partition, std::move(file_bucket_path_mapping),
                                            std::move(file_branch_mapping));
}

Result<std::shared_ptr<IndexedSplitImpl>> ReadIndexedSplitPayload(
    int64_t magic, DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    if (magic != IndexedSplitImpl::MAGIC) {
        return Status::Invalid(fmt::format("Corrupted IndexedSplit: wrong magic number {}", magic));
    }

    PAIMON_ASSIGN_OR_RAISE(int32_t version, in->ReadValue<int32_t>());
    if (version != IndexedSplitImpl::VERSION) {
        return Status::Invalid(fmt::format("Unsupported IndexedSplit version: {}", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t data_split_magic, in->ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> data_split,
                           ReadDataSplitWithoutMagicNumber(data_split_magic, in, pool));
    PAIMON_ASSIGN_OR_RAISE(int32_t range_size, in->ReadValue<int32_t>());
    if (range_size < 0) {
        return Status::Invalid(fmt::format("invalid IndexedSplit range size: {}", range_size));
    }
    std::vector<Range> row_ranges;
    row_ranges.reserve(range_size);
    for (int32_t i = 0; i < range_size; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int64_t range_from, in->ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t range_to, in->ReadValue<int64_t>());
        row_ranges.emplace_back(range_from, range_to);
    }

    std::vector<float> scores;
    PAIMON_ASSIGN_OR_RAISE(bool has_scores, in->ReadValue<bool>());
    if (has_scores) {
        PAIMON_ASSIGN_OR_RAISE(int32_t scores_length, in->ReadValue<int32_t>());
        if (scores_length < 0) {
            return Status::Invalid(
                fmt::format("invalid IndexedSplit scores length: {}", scores_length));
        }
        scores.resize(scores_length);
        for (int32_t i = 0; i < scores_length; ++i) {
            PAIMON_ASSIGN_OR_RAISE(float score, in->ReadValue<float>());
            scores[i] = score;
        }
    }

    return std::make_shared<IndexedSplitImpl>(data_split, row_ranges, scores);
}

Result<std::shared_ptr<Split>> ReadSplitSerializerPayload(DataInputStream* in,
                                                          const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(int32_t version, in->ReadValue<int32_t>());
    if (version != kSplitSerializerVersion) {
        return Status::Invalid(fmt::format("unsupported split serializer version: {}", version));
    }

    PAIMON_ASSIGN_OR_RAISE(int32_t type, in->ReadValue<int32_t>());
    switch (type) {
        case kSplitSerializerDataSplit: {
            PAIMON_ASSIGN_OR_RAISE(int64_t data_split_magic, in->ReadValue<int64_t>());
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> data_split,
                                   ReadDataSplitWithoutMagicNumber(data_split_magic, in, pool));
            PAIMON_RETURN_NOT_OK(ValidateFullyRead("DataSplit", in));
            return std::static_pointer_cast<Split>(data_split);
        }
        case kSplitSerializerIndexedSplit: {
            PAIMON_ASSIGN_OR_RAISE(int64_t indexed_split_magic, in->ReadValue<int64_t>());
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexedSplitImpl> indexed_split,
                                   ReadIndexedSplitPayload(indexed_split_magic, in, pool));
            PAIMON_RETURN_NOT_OK(ValidateFullyRead("IndexedSplit", in));
            return std::static_pointer_cast<Split>(indexed_split);
        }
        case kSplitSerializerChainSplit: {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ChainSplitImpl> chain_split,
                                   ReadChainSplitPayload(in, pool));
            return std::static_pointer_cast<Split>(chain_split);
        }
        case kSplitSerializerFallbackDataSplit: {
            PAIMON_ASSIGN_OR_RAISE(int64_t data_split_magic, in->ReadValue<int64_t>());
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> data_split,
                                   ReadDataSplitWithoutMagicNumber(data_split_magic, in, pool));
            PAIMON_ASSIGN_OR_RAISE(bool is_fallback, in->ReadValue<bool>());
            PAIMON_RETURN_NOT_OK(ValidateFullyRead("FallbackDataSplit", in));
            return std::static_pointer_cast<Split>(
                std::make_shared<FallbackDataSplit>(data_split, is_fallback));
        }
        default:
            return Status::Invalid(fmt::format("unsupported split serializer type: {}", type));
    }
}

}  // namespace

Result<std::string> Split::Serialize(const std::shared_ptr<Split>& split,
                                     const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    if (auto fallback_data_split = std::dynamic_pointer_cast<FallbackDataSplit>(split)) {
        if (std::dynamic_pointer_cast<ChainSplitImpl>(fallback_data_split->GetSplit())) {
            return Status::Invalid("FallbackDataSplit cannot serialize a wrapped ChainSplit");
        }
        auto data_split_impl =
            std::dynamic_pointer_cast<DataSplitImpl>(fallback_data_split->GetSplit());
        if (!data_split_impl) {
            return Status::Invalid("inner split in FallbackDataSplit is supposed to be DataSplit");
        }
        PAIMON_RETURN_NOT_OK(WriteDataSplit(data_split_impl, &out, pool));
        out.WriteValue<bool>(fallback_data_split->IsFallback());
    } else if (auto chain_split_impl = std::dynamic_pointer_cast<ChainSplitImpl>(split)) {
        out.WriteValue<int64_t>(kSplitSerializerMagic);
        out.WriteValue<int32_t>(kSplitSerializerVersion);
        out.WriteValue<int32_t>(kSplitSerializerChainSplit);
        PAIMON_RETURN_NOT_OK(WriteChainSplitPayload(chain_split_impl, &out, pool));
    } else if (auto data_split_impl = std::dynamic_pointer_cast<DataSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(WriteDataSplit(data_split_impl, &out, pool));
    } else if (auto indexed_split_impl = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        out.WriteValue<int64_t>(IndexedSplitImpl::MAGIC);
        out.WriteValue<int32_t>(IndexedSplitImpl::VERSION);
        if (std::dynamic_pointer_cast<ChainSplitImpl>(indexed_split_impl->GetDataSplit())) {
            return Status::Invalid("IndexedSplit cannot serialize a wrapped ChainSplit");
        }
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
        return Status::Invalid(
            "invalid split, cannot cast to FallbackDataSplit, ChainSplit, DataSplit or "
            "IndexedSplit");
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

    if (magic == kSplitSerializerMagic) {
        return ReadSplitSerializerPayload(&in, pool);
    } else if (magic == IndexedSplitImpl::MAGIC) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexedSplitImpl> indexed_split,
                               ReadIndexedSplitPayload(magic, &in, pool));
        // TODO(lisizhuo.lsz): support fallback split in IndexedSplit
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
        PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
        if (pos == stream_length) {
            return indexed_split;
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
        auto fully_read = ValidateFullyRead("DataSplit", &in);
        if (fully_read.ok()) {
            return data_split;
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
        PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
        if (pos == stream_length - 1) {
            PAIMON_ASSIGN_OR_RAISE(bool is_fallback, in.ReadValue<bool>());
            return std::make_shared<FallbackDataSplit>(data_split, is_fallback);
        }
        return fully_read;
    }
    return Status::Invalid("invalid split, must be SplitSerializer, DataSplit or IndexedSplit");
}
}  // namespace paimon
