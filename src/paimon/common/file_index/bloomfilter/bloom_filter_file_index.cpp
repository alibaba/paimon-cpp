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

#include "paimon/common/file_index/bloomfilter/bloom_filter_file_index.h"

#include <cstddef>
#include <functional>
#include <utility>

#include "fmt/format.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

BloomFilterFileIndex::BloomFilterFileIndex(const std::map<std::string, std::string>& options) {}
Result<std::shared_ptr<FileIndexReader>> BloomFilterFileIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, int32_t start, int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BloomFilterFileIndexReader, supposed to have single "
            "field.");
    }
    auto arrow_type = arrow_schema->field(0)->type();

    PAIMON_RETURN_NOT_OK(input_stream->Seek(start, SeekOrigin::FS_SEEK_SET));
    auto bytes = std::make_shared<Bytes>(length, pool.get());
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_read_len,
                           input_stream->Read(bytes->data(), bytes->size()));
    if (static_cast<size_t>(actual_read_len) != bytes->size()) {
        return Status::Invalid(
            fmt::format("create reader for BloomFilterFileIndex failed, expected read len "
                        "{}, actual read len {}",
                        bytes->size(), actual_read_len));
    }
    return BloomFilterFileIndexReader::Create(arrow_type, bytes);
}

Result<std::shared_ptr<BloomFilterFileIndexReader>> BloomFilterFileIndexReader::Create(
    const std::shared_ptr<arrow::DataType>& arrow_type, const std::shared_ptr<Bytes>& bytes) {
    // compatible with java, little endian
    const char* data = bytes->data();
    auto num_hash_functions =
        static_cast<int32_t>((static_cast<uint32_t>(static_cast<uint8_t>(data[0])) << 24) |
                             (static_cast<uint32_t>(static_cast<uint8_t>(data[1])) << 16) |
                             (static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 8) |
                             static_cast<uint32_t>(static_cast<uint8_t>(data[3])));
    PAIMON_ASSIGN_OR_RAISE(FastHash::HashFunction hash_function,
                           FastHash::GetHashFunction(arrow_type));
    auto bit_set = std::make_unique<BloomFilter64::BitSet>(bytes, /*offset=*/sizeof(int32_t));
    return std::shared_ptr<BloomFilterFileIndexReader>(new BloomFilterFileIndexReader(
        hash_function, BloomFilter64(num_hash_functions, std::move(bit_set))));
}

BloomFilterFileIndexReader::BloomFilterFileIndexReader(const FastHash::HashFunction& hash_function,
                                                       BloomFilter64&& filter)
    : hash_function_(hash_function), filter_(std::move(filter)) {}

Result<std::shared_ptr<FileIndexResult>> BloomFilterFileIndexReader::VisitEqual(
    const Literal& literal) {
    // This returns `Remain` to align with the current Java implementation in BF index, even though
    // its predicate semantics are inconsistent here. In practice, equality tests in predicate
    // evaluation always return false when the literal is null. See
    // `null_false_leaf_binary_function.h`.
    if (literal.IsNull()) {
        return FileIndexResult::Remain();
    }
    int64_t hash = hash_function_(literal);
    return filter_.TestHash(hash) ? FileIndexResult::Remain() : FileIndexResult::Skip();
}

}  // namespace paimon
