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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/result.h"

namespace paimon {

class ChainSplitFilePathFactory : public DataFilePathFactory {
 public:
    static Result<std::shared_ptr<ChainSplitFilePathFactory>> Create(
        const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
        std::unordered_map<std::string, std::string> file_bucket_path_mapping);

    explicit ChainSplitFilePathFactory(
        std::unordered_map<std::string, std::string> file_bucket_path_mapping);

    std::string ToPath(const std::shared_ptr<DataFileMeta>& file_meta) const override;
    std::string ToAlignedPath(const std::string& file_name,
                              const std::shared_ptr<DataFileMeta>& aligned) const override;

 private:
    std::unordered_map<std::string, std::string> file_bucket_path_mapping_;
};

}  // namespace paimon
