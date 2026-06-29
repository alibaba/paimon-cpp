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

#include "paimon/core/io/chain_data_file_path_factory.h"

#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"

namespace paimon {

ChainDataFilePathFactory::ChainDataFilePathFactory(
    std::shared_ptr<DataFilePathFactory> fallback,
    std::unordered_map<std::string, std::string> file_bucket_path_mapping)
    : fallback_(std::move(fallback)),
      file_bucket_path_mapping_(std::move(file_bucket_path_mapping)) {}

std::string ChainDataFilePathFactory::ToPath(const std::shared_ptr<DataFileMeta>& file_meta) const {
    if (file_meta->external_path) {
        return file_meta->external_path.value();
    }

    auto it = file_bucket_path_mapping_.find(file_meta->file_name);
    if (it != file_bucket_path_mapping_.end()) {
        return PathUtil::JoinPath(it->second, file_meta->file_name);
    }

    return fallback_->ToPath(file_meta);
}

}  // namespace paimon
