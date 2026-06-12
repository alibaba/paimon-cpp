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

#include <atomic>
#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// Provider for external data paths.
class ExternalPathProvider {
 public:
    static Result<std::unique_ptr<ExternalPathProvider>> Create(
        const std::vector<std::string>& external_table_paths,
        const std::string& relative_bucket_path) {
        if (external_table_paths.empty()) {
            return Status::Invalid("external table paths cannot be empty");
        }
        return std::unique_ptr<ExternalPathProvider>(
            new ExternalPathProvider(external_table_paths, relative_bucket_path));
    }

    /// Get the next external data path.
    ///
    /// @return the next external data path
    std::string GetNextExternalDataPath(const std::string& file_name) {
        size_t position = (++position_) % external_table_paths_.size();
        return PathUtil::JoinPath(
            PathUtil::JoinPath(external_table_paths_[position], relative_bucket_path_), file_name);
    }

 private:
    ExternalPathProvider(const std::vector<std::string>& external_table_paths,
                         const std::string& relative_bucket_path)
        : external_table_paths_(external_table_paths), relative_bucket_path_(relative_bucket_path) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(0, external_table_paths_.size() - 1);
        position_ = dist(gen);
    }

 private:
    std::vector<std::string> external_table_paths_;
    std::string relative_bucket_path_;
    std::atomic<size_t> position_;
};
}  // namespace paimon
