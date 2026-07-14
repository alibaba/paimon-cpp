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
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/fs/external_path_provider.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/utils/path_factory.h"

namespace paimon {

class Status;
struct DataFileMeta;

/// %Factory which produces new paths and converts paths for data files.
class DataFilePathFactory : public PathFactory {
 public:
    static const char CHANGELOG_FILE_PREFIX[];
    static const char INDEX_PATH_SUFFIX[];

    Status Init(const std::string& parent, const std::string& format_identifier,
                const std::string& data_file_prefix,
                std::unique_ptr<ExternalPathProvider>&& external_path_provider);

    const std::string& Parent() const {
        return parent_;
    }

    const std::string& DataFilePrefix() const {
        return data_file_prefix_;
    }

    std::string NewPath() const override {
        return NewPath(data_file_prefix_);
    }

    std::string NewChangelogPath() const {
        return NewPath(std::string(CHANGELOG_FILE_PREFIX));
    }

    std::string NewBlobPath() const {
        return NewPathFromName(NewFileName(data_file_prefix_, ".blob"));
    }

    std::string NewPathFromName(const std::string& file_name) const {
        if (external_path_provider_ != nullptr) {
            return external_path_provider_->GetNextExternalDataPath(file_name);
        }
        return PathUtil::JoinPath(parent_, file_name);
    }

    std::string ToPath(const std::string& file_name) const override;
    std::string ToPath(const std::shared_ptr<DataFileMeta>& file_meta) const;

    const std::string& GetUUID() const {
        return uuid_;
    }

    std::string ToFileIndexPath(const std::string& file_path) const;
    std::string ToAlignedPath(const std::string& file_name,
                              const std::shared_ptr<DataFileMeta>& aligned) const;

    std::vector<std::string> CollectFiles(const std::shared_ptr<DataFileMeta>& file_meta) const;
    bool IsExternalPath() const {
        return external_path_provider_ != nullptr;
    }

 private:
    std::string NewPath(const std::string& prefix) const;

    std::string NewFileName(const std::string& prefix) const {
        // TODO(yonghao.fyh): add compress extension as java paimon if needed
        std::string extension = "." + format_identifier_;
        return NewFileName(prefix, extension);
    }

    std::string NewFileName(const std::string& prefix, const std::string& extension) const {
        return prefix + uuid_ + "-" + std::to_string(path_count_.fetch_add(1)) + extension;
    }

 private:
    std::string parent_;
    std::string uuid_;
    mutable std::atomic<int32_t> path_count_;
    std::string format_identifier_;
    std::string data_file_prefix_;
    std::unique_ptr<ExternalPathProvider> external_path_provider_;
};

}  // namespace paimon
