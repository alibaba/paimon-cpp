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

#include "paimon/common/global_index/btree/btree_global_index_writer.h"

#include <map>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/global_index/btree/btree_defs.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/preconditions.h"
#include "paimon/memory/bytes.h"
namespace paimon {
Result<std::shared_ptr<BTreeGlobalIndexWriter>> BTreeGlobalIndexWriter::Create(
    const std::string& field_name, const std::shared_ptr<arrow::StructType>& arrow_type,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer, int32_t block_size,
    const std::shared_ptr<paimon::BlockCompressionFactory>& compression_factory,
    const std::shared_ptr<MemoryPool>& pool) {
    auto key_field = arrow_type->GetFieldByName(field_name);
    PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
        key_field,
        fmt::format("field {} not in arrow_array when Create BTreeGlobalIndexWriter", field_name)));
    PAIMON_ASSIGN_OR_RAISE(std::string index_file_name,
                           file_writer->NewFileName(BtreeDefs::kIdentifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output_stream,
                           file_writer->NewOutputStream(index_file_name));
    auto sst_file_writer = std::make_unique<SstFileWriter>(output_stream, /*bloom_filter=*/nullptr,
                                                           block_size, compression_factory, pool);
    return std::shared_ptr<BTreeGlobalIndexWriter>(new BTreeGlobalIndexWriter(
        field_name, arrow_type, key_field->type(), file_writer, index_file_name, output_stream,
        std::move(sst_file_writer), pool));
}

BTreeGlobalIndexWriter::BTreeGlobalIndexWriter(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    const std::shared_ptr<arrow::DataType>& key_type,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer, const std::string& index_file_name,
    const std::shared_ptr<OutputStream>& output_stream, std::unique_ptr<SstFileWriter>&& sst_writer,
    const std::shared_ptr<MemoryPool>& pool)
    : field_name_(field_name),
      arrow_type_(arrow_type),
      key_type_(key_type),
      pool_(pool),
      file_writer_(file_writer),
      index_file_name_(index_file_name),
      output_stream_(output_stream),
      sst_writer_(std::move(sst_writer)) {}

Status BTreeGlobalIndexWriter::AddBatch(::ArrowArray* arrow_array,
                                        std::vector<int64_t>&& relative_row_ids) {
    PAIMON_RETURN_NOT_OK(GlobalIndexUtils::CheckRelativeRowIds(
        arrow_array, relative_row_ids, /*expected_next_row_id=*/std::nullopt));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
        struct_array, "arrow array must be struct array when AddBatch to BTreeGlobalIndexWriter"));
    auto value_array = struct_array->GetFieldByName(field_name_);
    PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
        value_array,
        fmt::format("field {} not in arrow_array when AddBatch to BTreeGlobalIndexWriter",
                    field_name_)));

    // Process each element in the array
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> literals,
                           LiteralConverter::ConvertLiteralsFromArray(*value_array,
                                                                      /*own_data=*/true));
    for (size_t i = 0; i < literals.size(); ++i) {
        int64_t row_id = relative_row_ids[i];
        const auto& literal = literals[i];
        if (literal.IsNull()) {
            // Track null values
            null_bitmap_.Add(row_id);
            continue;
        }
        if (last_key_) {
            PAIMON_ASSIGN_OR_RAISE(int32_t cmp, literal.CompareTo(last_key_.value()));
            if (cmp > 0) {
                PAIMON_RETURN_NOT_OK(Flush());
            } else if (cmp < 0) {
                return Status::Invalid(
                    fmt::format("Users must keep written keys monotonically incremental in "
                                "BTreeGlobalIndexWriter, current literal {}, last_key {}",
                                literal.ToString(), last_key_.value().ToString()));
            }
        }
        last_key_ = literal;
        current_row_ids_.push_back(row_id);
        if (!first_key_) {
            first_key_ = literal;
        }
    }
    return Status::OK();
}

Status BTreeGlobalIndexWriter::Flush() {
    if (current_row_ids_.empty()) {
        return Status::OK();
    }
    MemorySliceOutput output(current_row_ids_.size() * 9 + 5, pool_.get());
    if (current_row_ids_.size() > INT32_MAX) {
        return Status::Invalid("invalid row id numbers, exceed INT32_MAX");
    }
    PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(static_cast<int32_t>(current_row_ids_.size())));
    for (int64_t row_id : current_row_ids_) {
        PAIMON_RETURN_NOT_OK(output.WriteVarLenLong(row_id));
    }
    current_row_ids_.clear();
    assert(last_key_);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> key_bytes,
                           KeySerializer::SerializeKey(last_key_.value(), key_type_, pool_.get()));
    return sst_writer_->Write(std::move(key_bytes), output.ToSlice().CopyBytes(pool_.get()));
}

Result<std::optional<BlockHandle>> BTreeGlobalIndexWriter::WriteNullBitmap(
    const std::shared_ptr<OutputStream>& out) {
    if (null_bitmap_.IsEmpty()) {
        return std::optional<BlockHandle>();
    }
    std::shared_ptr<Bytes> bitmap_bytes = null_bitmap_.Serialize(pool_.get());
    uint32_t crc = CRC32C::calculate(bitmap_bytes->data(), bitmap_bytes->size());

    MemorySliceOutput slice_out(bitmap_bytes->size() + 4, pool_.get());
    slice_out.WriteBytes(bitmap_bytes);
    slice_out.WriteValue<int32_t>(static_cast<int32_t>(crc));

    // Get current position for the block handle
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, out->GetPos());
    PAIMON_RETURN_NOT_OK(sst_writer_->WriteSlice(slice_out.ToSlice()));
    return std::optional<BlockHandle>(BlockHandle(offset, bitmap_bytes->size()));
}

Result<std::vector<GlobalIndexIOMeta>> BTreeGlobalIndexWriter::Finish() {
    // write remaining row ids
    PAIMON_RETURN_NOT_OK(Flush());

    // Flush any remaining data in the data block writer
    PAIMON_RETURN_NOT_OK(sst_writer_->Flush());

    // Write null bitmap first
    PAIMON_ASSIGN_OR_RAISE(std::optional<BlockHandle> null_bitmap_handle,
                           WriteNullBitmap(output_stream_));
    // write bloom filter (currently is always null, but we could add it for equal
    // and in condition.)
    PAIMON_ASSIGN_OR_RAISE(std::optional<BloomFilterHandle> bloom_filter_handle,
                           sst_writer_->WriteBloomFilter());
    // Write index block
    PAIMON_ASSIGN_OR_RAISE(BlockHandle index_block_handle, sst_writer_->WriteIndexBlock());

    // Write BTree file footer
    auto footer = std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle,
                                                    null_bitmap_handle);
    auto footer_slice = BTreeFileFooter::Write(footer, pool_.get());
    PAIMON_RETURN_NOT_OK(sst_writer_->WriteSlice(footer_slice));

    PAIMON_RETURN_NOT_OK(output_stream_->Close());

    if (!first_key_ && null_bitmap_.IsEmpty()) {
        return Status::Invalid("Should never write an empty btree index file.");
    }

    // Get file size
    std::shared_ptr<Bytes> first_key_bytes;
    std::shared_ptr<Bytes> last_key_bytes;
    if (first_key_) {
        PAIMON_ASSIGN_OR_RAISE(first_key_bytes, KeySerializer::SerializeKey(
                                                    first_key_.value(), key_type_, pool_.get()));
    }
    if (last_key_) {
        PAIMON_ASSIGN_OR_RAISE(
            last_key_bytes, KeySerializer::SerializeKey(last_key_.value(), key_type_, pool_.get()));
    }
    // Create index meta
    auto index_meta =
        std::make_shared<BTreeIndexMeta>(first_key_bytes, last_key_bytes, !null_bitmap_.IsEmpty());
    auto meta_bytes = index_meta->Serialize(pool_.get());

    // Create GlobalIndexIOMeta
    std::string file_path = file_writer_->ToPath(index_file_name_);
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_writer_->GetFileSize(index_file_name_));
    GlobalIndexIOMeta io_meta(file_path, file_size, meta_bytes);
    return std::vector<GlobalIndexIOMeta>{io_meta};
}

}  // namespace paimon
