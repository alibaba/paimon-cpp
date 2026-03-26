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

#include <arrow/c/bridge.h>

#include <unordered_map>

#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/memory/bytes.h"

namespace paimon {

BTreeGlobalIndexWriter::BTreeGlobalIndexWriter(
    const std::string& field_name,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool,
    int32_t block_size,
    int64_t expected_entries)
    : field_name_(field_name),
      file_writer_(file_writer),
      pool_(pool),
      block_size_(block_size),
      expected_entries_(expected_entries),
      null_bitmap_(std::make_shared<RoaringNavigableMap64>()),
      has_nulls_(false),
      current_row_id_(0),
      bloom_filter_(std::make_shared<BloomFilter>(expected_entries, 0.01)) {}

Status BTreeGlobalIndexWriter::AddBatch(::ArrowArray* arrow_array) {
    if (!arrow_array) {
        return Status::Invalid("ArrowArray is null");
    }

    // Import Arrow array
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow::null()));

    // Initialize SST writer on first batch
    if (!sst_writer_) {
        PAIMON_ASSIGN_OR_RAISE(file_name_, file_writer_->NewFileName(field_name_));
        PAIMON_ASSIGN_OR_RAISE(output_stream_, file_writer_->NewOutputStream(file_name_));
        sst_writer_ = std::make_unique<SstFileWriter>(output_stream_, pool_, bloom_filter_,
                                                       block_size_, nullptr);
    }

    // Group row IDs by key value
    std::unordered_map<std::string, std::vector<int64_t>> key_to_row_ids;

    // Process each element in the array
    for (int64_t i = 0; i < array->length(); ++i) {
        int64_t row_id = current_row_id_ + i;

        if (array->IsNull(i)) {
            // Track null values
            null_bitmap_->Add(row_id);
            has_nulls_ = true;
            continue;
        }

        // Convert array element to string key
        // For simplicity, we use string representation for all types
        // TODO: Support type-specific serialization for better comparison
        std::string key_str;

        // Get the value as string based on array type
        auto type_id = array->type_id();
        switch (type_id) {
            case arrow::Type::STRING:
            case arrow::Type::BINARY: {
                auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
                key_str = std::string(str_array->GetView(i));
                break;
            }
            case arrow::Type::INT32: {
                auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
                key_str = std::to_string(int_array->Value(i));
                break;
            }
            case arrow::Type::INT64: {
                auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
                key_str = std::to_string(int_array->Value(i));
                break;
            }
            case arrow::Type::FLOAT: {
                auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
                key_str = std::to_string(float_array->Value(i));
                break;
            }
            case arrow::Type::DOUBLE: {
                auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                key_str = std::to_string(double_array->Value(i));
                break;
            }
            case arrow::Type::BOOL: {
                auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                key_str = bool_array->Value(i) ? "1" : "0";
                break;
            }
            case arrow::Type::DATE32: {
                auto date_array = std::static_pointer_cast<arrow::Date32Array>(array);
                key_str = std::to_string(date_array->Value(i));
                break;
            }
            case arrow::Type::TIMESTAMP: {
                auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(array);
                key_str = std::to_string(ts_array->Value(i));
                break;
            }
            default:
                return Status::NotImplemented("Unsupported arrow type for BTree index: " +
                                              array->type()->ToString());
        }

        key_to_row_ids[key_str].push_back(row_id);
    }

    // Write each key and its row IDs to the SST file
    for (const auto& [key_str, row_ids] : key_to_row_ids) {
        auto key_bytes = Bytes::AllocateBytes(key_str, pool_.get());
        auto key = std::shared_ptr<Bytes>(key_bytes.release());

        // Track first and last keys
        if (!first_key_) {
            first_key_ = key;
        }
        last_key_ = key;

        // Write key-value pair
        PAIMON_RETURN_NOT_OK(WriteKeyValue(key, row_ids));
    }

    current_row_id_ += array->length();
    return Status::OK();
}

Status BTreeGlobalIndexWriter::WriteKeyValue(const std::shared_ptr<Bytes>& key,
                                              const std::vector<int64_t>& row_ids) {
    auto value = SerializeRowIds(row_ids);
    // Copy key since we can't move from a const reference
    auto key_copy = key;
    return sst_writer_->Write(std::move(key_copy), std::move(value));
}

std::shared_ptr<Bytes> BTreeGlobalIndexWriter::SerializeRowIds(const std::vector<int64_t>& row_ids) {
    // Format: [num_row_ids (varint)][row_id1 (int64)][row_id2]...
    int32_t estimated_size = 10 + row_ids.size() * 8;  // Conservative estimate
    auto output = std::make_shared<MemorySliceOutput>(estimated_size, pool_.get());

    output->WriteVarLenLong(static_cast<int64_t>(row_ids.size()));
    for (int64_t row_id : row_ids) {
        output->WriteValue(row_id);
    }

    auto slice = output->ToSlice();
    return slice->CopyBytes(pool_.get());
}

Result<std::shared_ptr<BlockHandle>> BTreeGlobalIndexWriter::WriteNullBitmap(
    const std::shared_ptr<OutputStream>& out) {
    if (!has_nulls_ || null_bitmap_->IsEmpty()) {
        return std::shared_ptr<BlockHandle>(nullptr);
    }

    // Serialize null bitmap
    std::vector<uint8_t> bitmap_data = null_bitmap_->Serialize();
    if (bitmap_data.empty()) {
        return std::shared_ptr<BlockHandle>(nullptr);
    }

    // Get current position for the block handle
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, out->GetPos());

    // Write bitmap data
    PAIMON_RETURN_NOT_OK(out->Write(reinterpret_cast<const char*>(bitmap_data.data()),
                                    bitmap_data.size()));

    // Calculate and write CRC32C
    uint32_t crc = CRC32C::calculate(reinterpret_cast<const char*>(bitmap_data.data()),
                                     bitmap_data.size());
    PAIMON_RETURN_NOT_OK(out->Write(reinterpret_cast<const char*>(&crc), sizeof(crc)));

    return std::make_shared<BlockHandle>(offset, bitmap_data.size());
}

Result<std::vector<GlobalIndexIOMeta>> BTreeGlobalIndexWriter::Finish() {
    if (!sst_writer_) {
        // No data was written, return empty metadata
        return std::vector<GlobalIndexIOMeta>();
    }

    // Flush any remaining data in the data block writer
    PAIMON_RETURN_NOT_OK(sst_writer_->Flush());

    // Write index block
    PAIMON_ASSIGN_OR_RAISE(auto index_block_handle, sst_writer_->WriteIndexBlock());

    // Write bloom filter
    PAIMON_ASSIGN_OR_RAISE(auto bloom_filter_handle, sst_writer_->WriteBloomFilter());

    // Write null bitmap
    PAIMON_ASSIGN_OR_RAISE(auto null_bitmap_handle, WriteNullBitmap(output_stream_));

    // Write BTree file footer
    auto footer = std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle,
                                                     null_bitmap_handle);
    auto footer_slice = BTreeFileFooter::Write(footer, pool_.get());
    auto footer_bytes = footer_slice->CopyBytes(pool_.get());
    PAIMON_RETURN_NOT_OK(output_stream_->Write(footer_bytes->data(), footer_bytes->size()));

    // Close the output stream
    PAIMON_RETURN_NOT_OK(output_stream_->Close());

    // Get file size
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_writer_->GetFileSize(file_name_));

    // Create index meta
    auto index_meta = std::make_shared<BTreeIndexMeta>(first_key_, last_key_, has_nulls_);
    auto meta_bytes = index_meta->Serialize(pool_.get());

    // Create GlobalIndexIOMeta
    std::string file_path = file_writer_->ToPath(file_name_);
    GlobalIndexIOMeta io_meta(file_path, file_size, current_row_id_ - 1, meta_bytes);

    return std::vector<GlobalIndexIOMeta>{io_meta};
}

}  // namespace paimon