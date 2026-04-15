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

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index_factory.h"

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"

namespace paimon {

RangeBitmapFileIndexFactory::~RangeBitmapFileIndexFactory() = default;

Result<std::unique_ptr<FileIndexer>> RangeBitmapFileIndexFactory::Create(
    const std::map<std::string, std::string>& options) const {
    return std::make_unique<RangeBitmapFileIndex>(options);
}

REGISTER_PAIMON_FACTORY(RangeBitmapFileIndexFactory);

}  // namespace paimon
