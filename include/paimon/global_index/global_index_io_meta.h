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

#include <memory>
#include <string>

#include "paimon/memory/bytes.h"
#include "paimon/utils/range.h"

namespace paimon {
/// Metadata describing a single file entry in a global index.
struct PAIMON_EXPORT GlobalIndexIOMeta {
    GlobalIndexIOMeta(const std::string& _file_path, int64_t _file_size, int64_t _range_end,
                      const std::shared_ptr<Bytes>& _metadata)
        : file_path(_file_path),
          file_size(_file_size),
          range_end(_range_end),
          metadata(_metadata) {}

    std::string file_path;
    int64_t file_size;
    /// The inclusive range end covered by this file (i.e., the last local row id).
    int64_t range_end;
    /// Optional binary metadata associated with the file, such as serialized
    /// secondary index structures or inline index bytes.
    /// May be null if no additional metadata is available.
    std::shared_ptr<Bytes> metadata;
};

}  // namespace paimon
