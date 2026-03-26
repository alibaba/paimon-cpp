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

#include "paimon/common/global_index/rangebitmap/range_bitmap_global_index_factory.h"

#include <utility>

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"
#include "paimon/common/global_index/rangebitmap/range_bitmap_global_index.h"

namespace paimon {

const char RangeBitmapGlobalIndexFactory::IDENTIFIER[] = "range-bitmap-global";

Result<std::unique_ptr<GlobalIndexer>> RangeBitmapGlobalIndexFactory::Create(
    const std::map<std::string, std::string>& options) const {
    auto range_bitmap_file_index = std::make_shared<RangeBitmapFileIndex>(options);
    return std::make_unique<RangeBitmapGlobalIndex>(range_bitmap_file_index);
}

REGISTER_PAIMON_FACTORY(RangeBitmapGlobalIndexFactory);

}  // namespace paimon
