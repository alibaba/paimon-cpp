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

#include <map>

#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/memory/bytes.h"

namespace paimon {

Result<std::shared_ptr<BTreeGlobalIndexWriter>> BTreeGlobalIndexWriter::Create(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<paimon::BlockCompressionFactory>& compression_factory,
    const std::shared_ptr<MemoryPool>& pool, int32_t block_size) {
    // Import schema to get the field type
    std::shared_ptr<arrow::DataType> arrow_type;
    if (arrow_schema) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                          arrow::ImportSchema(arrow_schema));
        if (schema->num_fields() > 0) {
            arrow_type = schema->field(0)->type();
        }
    }

    auto writer = std::shared_ptr<BTreeGlobalIndexWriter>(new BTreeGlobalIndexWriter(
        field_name, std::move(arrow_type), file_writer, compression_factory, pool, block_size));

    // Initialize SST writer
    if (!file_writer) {
        return Status::Invalid("file_writer is null");
    }
    PAIMON_ASSIGN_OR_RAISE(writer->file_name_, file_writer->NewFileName("btree"));
    PAIMON_ASSIGN_OR_RAISE(writer->output_stream_,
                           file_writer->NewOutputStream(writer->file_name_));
    writer->sst_writer_ = std::make_unique<SstFileWriter>(writer->output_stream_, nullptr,
                                                          block_size, compression_factory, pool);

    return writer;
}

BTreeGlobalIndexWriter::BTreeGlobalIndexWriter(
    const std::string& field_name, std::shared_ptr<arrow::DataType> arrow_type,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<paimon::BlockCompressionFactory>& compression_factory,
    const std::shared_ptr<MemoryPool>& pool, int32_t block_size)
    : field_name_(field_name),
      arrow_type_(std::move(arrow_type)),
      pool_(pool),
      file_writer_(file_writer),
      null_bitmap_(std::make_shared<RoaringBitmap64>()),
      has_nulls_(false),
      current_row_id_(0) {}

Status BTreeGlobalIndexWriter::AddBatch(::ArrowArray* arrow_array) {
    if (!arrow_array) {
        return Status::Invalid("ArrowArray is null");
    }

    if (!arrow_type_) {
        return Status::Invalid(
            "Arrow type is not set. Please provide a valid ArrowSchema in constructor.");
    }

    // Import Arrow array with the correct type
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));

    // Group row IDs by key value
    // Use std::map with custom comparator for binary keys
    // Keys are stored in binary format to match Java's serialization
    std::map<std::shared_ptr<Bytes>, std::vector<int64_t>,
             std::function<bool(const std::shared_ptr<Bytes>&, const std::shared_ptr<Bytes>&)>>
        key_to_row_ids([this](const std::shared_ptr<Bytes>& a, const std::shared_ptr<Bytes>& b) {
            return CompareBinaryKeys(a, b) < 0;
        });

    // Process each element in the array
    for (int64_t i = 0; i < array->length(); ++i) {
        int64_t row_id = current_row_id_ + i;

        if (array->IsNull(i)) {
            // Track null values
            null_bitmap_->Add(row_id);
            has_nulls_ = true;
            continue;
        }

        // Convert array element to binary key
        // Use type-specific binary serialization to match Java format
        std::shared_ptr<Bytes> key_bytes;

        // Get the value as binary based on array type
        auto type_id = array->type_id();

        switch (type_id) {
            case arrow::Type::STRING:
            case arrow::Type::BINARY: {
                auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
                auto view = str_array->GetView(i);
                key_bytes = std::make_shared<Bytes>(view.size(), pool_.get());
                memcpy(key_bytes->data(), view.data(), view.size());
                break;
            }
            case arrow::Type::INT32: {
                auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
                int32_t value = int_array->Value(i);
                // Store as 4-byte little-endian to match Java's DataOutputStream.writeInt
                key_bytes = std::make_shared<Bytes>(sizeof(int32_t), pool_.get());
                memcpy(key_bytes->data(), &value, sizeof(int32_t));
                break;
            }
            case arrow::Type::INT64: {
                auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
                int64_t value = int_array->Value(i);
                // Store as 8-byte little-endian to match Java's DataOutputStream.writeLong
                key_bytes = std::make_shared<Bytes>(sizeof(int64_t), pool_.get());
                memcpy(key_bytes->data(), &value, sizeof(int64_t));
                break;
            }
            case arrow::Type::FLOAT: {
                auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
                float value = float_array->Value(i);
                // Store as 4-byte IEEE 754 to match Java's DataOutputStream.writeFloat
                key_bytes = std::make_shared<Bytes>(sizeof(float), pool_.get());
                memcpy(key_bytes->data(), &value, sizeof(float));
                break;
            }
            case arrow::Type::DOUBLE: {
                auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                double value = double_array->Value(i);
                // Store as 8-byte IEEE 754 to match Java's DataOutputStream.writeDouble
                key_bytes = std::make_shared<Bytes>(sizeof(double), pool_.get());
                memcpy(key_bytes->data(), &value, sizeof(double));
                break;
            }
            case arrow::Type::BOOL: {
                auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                bool value = bool_array->Value(i);
                // Store as single byte (0 or 1)
                key_bytes = std::make_shared<Bytes>(1, pool_.get());
                key_bytes->data()[0] = value ? 1 : 0;
                break;
            }
            case arrow::Type::DATE32: {
                auto date_array = std::static_pointer_cast<arrow::Date32Array>(array);
                int32_t value = date_array->Value(i);
                // Store as 4-byte int32 to match Java's writeInt for DATE type
                key_bytes = std::make_shared<Bytes>(sizeof(int32_t), pool_.get());
                memcpy(key_bytes->data(), &value, sizeof(int32_t));
                break;
            }
            case arrow::Type::TIMESTAMP: {
                auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(array);
                auto ts_type = std::static_pointer_cast<arrow::TimestampType>(array->type());
                int32_t precision = DateTimeUtils::GetPrecisionFromType(ts_type);
                auto time_type = DateTimeUtils::GetTimeTypeFromArrowType(ts_type);
                int64_t raw_value = ts_array->Value(i);
                auto [milli, nano] = DateTimeUtils::TimestampConverter(
                    raw_value, time_type, DateTimeUtils::TimeType::MILLISECOND,
                    DateTimeUtils::TimeType::NANOSECOND);
                if (Timestamp::IsCompact(precision)) {
                    // compact: writeLong(millisecond) — 8 bytes
                    key_bytes = std::make_shared<Bytes>(sizeof(int64_t), pool_.get());
                    memcpy(key_bytes->data(), &milli, sizeof(int64_t));
                } else {
                    // non-compact: writeLong(millisecond) + writeVarLenInt(nanoOfMillisecond)
                    MemorySliceOutput ts_out(13, pool_.get());
                    ts_out.WriteValue(milli);
                    PAIMON_RETURN_NOT_OK(ts_out.WriteVarLenInt(static_cast<int32_t>(nano)));
                    auto slice = ts_out.ToSlice();
                    key_bytes = slice.GetHeapMemory();
                }
                break;
            }
            default:
                return Status::NotImplemented("Unsupported arrow type for BTree index: " +
                                              array->type()->ToString());
        }

        key_to_row_ids[key_bytes].push_back(row_id);
    }

    // Write each key and its row IDs to the SST file
    for (const auto& [key_bytes, row_ids] : key_to_row_ids) {
        // Track first and last keys
        if (!first_key_) {
            first_key_ = key_bytes;
        }
        last_key_ = key_bytes;

        // Write key-value pair
        PAIMON_RETURN_NOT_OK(WriteKeyValue(key_bytes, row_ids));
    }

    current_row_id_ += array->length();
    return Status::OK();
}

Status BTreeGlobalIndexWriter::WriteKeyValue(std::shared_ptr<Bytes> key,
                                             const std::vector<int64_t>& row_ids) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> value, SerializeRowIds(row_ids));

    return sst_writer_->Write(std::move(key), std::move(value));
}

Result<std::shared_ptr<Bytes>> BTreeGlobalIndexWriter::SerializeRowIds(
    const std::vector<int64_t>& row_ids) {
    // Format: [num_row_ids (VarLenLong)][row_id1 (VarLenLong)][row_id2]...
    // Use VarLenLong for row IDs to match Java's MemorySliceOutput.writeVarLenLong
    int32_t estimated_size = 10 + row_ids.size() * 10;  // Conservative estimate
    auto output = std::make_shared<MemorySliceOutput>(estimated_size, pool_.get());

    PAIMON_RETURN_NOT_OK(output->WriteVarLenLong(static_cast<int64_t>(row_ids.size())));
    for (int64_t row_id : row_ids) {
        PAIMON_RETURN_NOT_OK(output->WriteVarLenLong(row_id));
    }

    auto slice = output->ToSlice();
    return slice.CopyBytes(pool_.get());
}

int32_t BTreeGlobalIndexWriter::CompareBinaryKeys(const std::shared_ptr<Bytes>& a,
                                                  const std::shared_ptr<Bytes>& b) const {
    if (!a || !b) return 0;
    size_t min_len = std::min(a->size(), b->size());
    int cmp = memcmp(a->data(), b->data(), min_len);
    if (cmp != 0) return cmp < 0 ? -1 : 1;
    if (a->size() < b->size()) return -1;
    if (a->size() > b->size()) return 1;
    return 0;
}

Result<std::shared_ptr<BlockHandle>> BTreeGlobalIndexWriter::WriteNullBitmap(
    const std::shared_ptr<OutputStream>& out) {
    if (!has_nulls_ || null_bitmap_->IsEmpty()) {
        return std::shared_ptr<BlockHandle>(nullptr);
    }

    // Serialize null bitmap
    auto bitmap_bytes = null_bitmap_->Serialize(pool_.get());
    if (!bitmap_bytes || bitmap_bytes->size() == 0) {
        return std::shared_ptr<BlockHandle>(nullptr);
    }

    // Get current position for the block handle
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, out->GetPos());

    // Write bitmap data
    PAIMON_RETURN_NOT_OK(out->Write(bitmap_bytes->data(), bitmap_bytes->size()));

    // Calculate and write CRC32C
    uint32_t crc = CRC32C::calculate(bitmap_bytes->data(), bitmap_bytes->size());
    PAIMON_RETURN_NOT_OK(out->Write(reinterpret_cast<const char*>(&crc), sizeof(crc)));

    return std::make_shared<BlockHandle>(offset, bitmap_bytes->size());
}

Result<std::vector<GlobalIndexIOMeta>> BTreeGlobalIndexWriter::Finish() {
    if (current_row_id_ == 0) {
        // No data was written, return empty metadata
        return std::vector<GlobalIndexIOMeta>();
    }

    // Flush any remaining data in the data block writer
    PAIMON_RETURN_NOT_OK(sst_writer_->Flush());

    // Write null bitmap first (matches Java write order: null bitmap → bloom filter → index block)
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockHandle> null_bitmap_handle,
                           WriteNullBitmap(output_stream_));

    // Write index block
    PAIMON_ASSIGN_OR_RAISE(BlockHandle index_block_handle, sst_writer_->WriteIndexBlock());

    // Write BTree file footer (no bloom filter)
    auto index_block_handle_ptr =
        std::make_shared<BlockHandle>(index_block_handle.Offset(), index_block_handle.Size());
    auto footer =
        std::make_shared<BTreeFileFooter>(nullptr, index_block_handle_ptr, null_bitmap_handle);
    auto footer_slice = BTreeFileFooter::Write(footer, pool_.get());
    auto footer_bytes = footer_slice.CopyBytes(pool_.get());
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
