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

#include "paimon/core/io/chain_split_file_path_factory.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/status.h"

namespace paimon {

Result<std::shared_ptr<ChainSplitFilePathFactory>> ChainSplitFilePathFactory::Create(
    const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    std::unordered_map<std::string, std::string> file_bucket_path_mapping) {
    for (const auto& file : data_files) {
        if (file->external_path) {
            continue;
        }
        if (file_bucket_path_mapping.find(file->file_name) == file_bucket_path_mapping.end()) {
            return Status::Invalid(
                fmt::format("bucket path is missing for ChainSplit file {}", file->file_name));
        }
    }
    return std::make_shared<ChainSplitFilePathFactory>(std::move(file_bucket_path_mapping));
}

ChainSplitFilePathFactory::ChainSplitFilePathFactory(
    std::unordered_map<std::string, std::string> file_bucket_path_mapping)
    : file_bucket_path_mapping_(std::move(file_bucket_path_mapping)) {}

std::string ChainSplitFilePathFactory::ToPath(
    const std::shared_ptr<DataFileMeta>& file_meta) const {
    if (file_meta->external_path) {
        return file_meta->external_path.value();
    }

    return PathUtil::JoinPath(file_bucket_path_mapping_.at(file_meta->file_name),
                              file_meta->file_name);
}

std::string ChainSplitFilePathFactory::ToAlignedPath(
    const std::string& file_name, const std::shared_ptr<DataFileMeta>& aligned) const {
    auto external_path = aligned->ExternalPathDir();
    if (external_path) {
        return PathUtil::JoinPath(external_path.value(), file_name);
    }
    return PathUtil::JoinPath(file_bucket_path_mapping_.at(aligned->file_name), file_name);
}

}  // namespace paimon
