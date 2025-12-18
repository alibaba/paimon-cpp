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
#include <string>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/manifest/file_kind.h"

namespace paimon {
/// Manifest entry for index file.
struct IndexManifestEntry {
    static const std::shared_ptr<arrow::DataType>& DataType() {
        static std::shared_ptr<arrow::DataType> data_type = arrow::struct_({
            arrow::field("_KIND", arrow::int8(), false),
            arrow::field("_PARTITION", arrow::binary(), false),
            arrow::field("_BUCKET", arrow::int32(), false),
            arrow::field("_INDEX_TYPE", arrow::utf8(), false),
            arrow::field("_FILE_NAME", arrow::utf8(), false),
            arrow::field("_FILE_SIZE", arrow::int64(), false),
            arrow::field("_ROW_COUNT", arrow::int64(), false),
            arrow::field("_DELETIONS_VECTORS_RANGES",
                         arrow::list(arrow::field("item", DeletionVectorMeta::DataType(), true)),
                         true),
            arrow::field("_EXTERNAL_PATH", arrow::utf8(), true),
            arrow::field("_GLOBAL_INDEX", GlobalIndexMeta::DataType(), true),
        });
        return data_type;
    }

    IndexManifestEntry(const FileKind& _kind, const BinaryRow& _partition, int32_t _bucket,
                       const std::shared_ptr<IndexFileMeta>& _index_file)
        : kind(_kind), partition(_partition), bucket(_bucket), index_file(_index_file) {}

    bool operator==(const IndexManifestEntry& other) const {
        if (this == &other) {
            return true;
        }
        return kind == other.kind && partition == other.partition && bucket == other.bucket &&
               *index_file == *(other.index_file);
    }

    std::string ToString() const {
        return fmt::format("IndexManifestEntry{{kind={}, partition={}, bucket={}, indexFile={}}}",
                           kind.ToByteValue(), partition.ToString(), bucket,
                           index_file->ToString());
    }

    FileKind kind;
    BinaryRow partition;
    int32_t bucket;
    std::shared_ptr<IndexFileMeta> index_file;
};
}  // namespace paimon
