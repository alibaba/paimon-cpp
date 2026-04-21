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
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class FileSystem;
class DataOutputStream;
class BucketedDvMaintainer;
struct DeletionFile;

/// The DeletionVector can efficiently record the positions of rows that are deleted in a file,
/// which can then be used to filter out deleted rows when processing the file.
class DeletionVector {
 public:
    using Factory = std::function<Result<std::shared_ptr<DeletionVector>>(const std::string&)>;

    static Factory CreateFactory(
        const std::shared_ptr<FileSystem>& file_system,
        const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
        const std::shared_ptr<MemoryPool>& pool);

    static Factory CreateFactory(const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer);

    virtual ~DeletionVector() = default;

    /// Marks the row at the specified position as deleted.
    ///
    /// @param position The position of the row to be marked as deleted.
    virtual Status Delete(int64_t position) = 0;

    /// Marks the row at the specified position as deleted.
    ///
    /// @param position The position of the row to be marked as deleted.
    /// @return true if the added position wasn't already deleted. False otherwise.
    virtual Result<bool> CheckedDelete(int64_t position) = 0;

    /// Checks if the row at the specified position is marked as deleted.
    ///
    /// @param position The position of the row to check.
    /// @return true if the row is marked as deleted, false otherwise.
    virtual Result<bool> IsDeleted(int64_t position) const = 0;

    Result<RoaringBitmap32> IsValid(int64_t start_position, int64_t length) const {
        RoaringBitmap32 is_valid;
        for (int64_t i = 0; i < length; i++) {
            PAIMON_ASSIGN_OR_RAISE(bool is_deleted, IsDeleted(start_position + i));
            if (!is_deleted) {
                is_valid.Add(i);
            }
        }
        return is_valid;
    }

    /// Merges another DeletionVector into this current one.
    ///
    /// This method combines the deletion positions from the other deletion vector
    /// with the current one, marking all positions deleted in either vector as deleted.
    ///
    /// @param deletion_vector The other DeletionVector to merge into this one.
    /// @return Status indicating success or failure of the merge operation.
    virtual Status Merge(const std::shared_ptr<DeletionVector>& deletion_vector) = 0;

    /// Determines if the deletion vector is empty, indicating no deletions.
    ///
    /// @return true if the deletion vector is empty, false if it contains deletions.
    virtual bool IsEmpty() const = 0;

    /// @return the number of distinct integers added to the DeletionVector.
    virtual int64_t GetCardinality() const = 0;

    /// Serializes the deletion vector.
    virtual Result<int32_t> SerializeTo(const std::shared_ptr<MemoryPool>& pool,
                                        DataOutputStream* out) = 0;

    /// Serializes the deletion vector to a byte array for storage or transmission.
    ///
    /// @return A byte array representing the serialized deletion vector.
    virtual Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeToBytes(
        const std::shared_ptr<MemoryPool>& pool) = 0;

    /// Deserializes a deletion vector from a byte array.
    ///
    /// @param bytes The byte array containing the serialized deletion vector.
    /// @return A DeletionVector instance that represents the deserialized data.
    static Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeserializeFromBytes(const Bytes* bytes,
                                                                          MemoryPool* pool);

    static Result<PAIMON_UNIQUE_PTR<DeletionVector>> Read(const FileSystem* file_system,
                                                          const DeletionFile& deletion_file,
                                                          MemoryPool* pool);

    static Result<PAIMON_UNIQUE_PTR<DeletionVector>> Read(DataInputStream* input_stream,
                                                          std::optional<int64_t> length,
                                                          MemoryPool* pool);

    static PAIMON_UNIQUE_PTR<DeletionVector> FromPrimitiveArray(const std::vector<char>& is_deleted,
                                                                MemoryPool* pool);
};

}  // namespace paimon
