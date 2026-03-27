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

#include "paimon/common/data/serializer/binary_row_serializer.h"

#include <algorithm>
#include <cassert>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"

namespace paimon {

Status BinaryRowSerializer::Serialize(const BinaryRow& record,
                                      MemorySegmentOutputStream* target) const {
    target->WriteValue<int32_t>(record.GetSizeInBytes());
    return SerializeWithoutLength(record, target);
}

Status BinaryRowSerializer::SerializeWithoutLength(const BinaryRow& record,
                                                   MemorySegmentOutputStream* target) const {
    target->Write(record.GetSegment(), record.GetOffset(), record.GetSizeInBytes());
    return Status::OK();
}

Result<BinaryRow> BinaryRowSerializer::Deserialize(DataInputStream* source) const {
    BinaryRow row(num_fields_);
    PAIMON_ASSIGN_OR_RAISE(int32_t read_length, source->ReadValue<int32_t>());
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(read_length, pool_.get());
    PAIMON_RETURN_NOT_OK(source->ReadBytes(bytes.get()));
    row.PointTo(MemorySegment::Wrap(bytes), 0, read_length);
    return row;
}

}  // namespace paimon
