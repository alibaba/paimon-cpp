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

#include <atomic>
#include <memory>
#include <string>

#include "paimon/common/utils/path_util.h"
#include "paimon/core/index/index_path_factory.h"

namespace paimon::test {

class MockIndexPathFactory : public IndexPathFactory {
 public:
    explicit MockIndexPathFactory(const std::string& index_path) : index_path_(index_path) {}

    std::string NewPath() const override {
        return PathUtil::JoinPath(index_path_, std::string(IndexPathFactory::INDEX_PREFIX) +
                                                   std::to_string(index_file_count_->fetch_add(1)));
    }
    std::string ToPath(const std::string& file_name) const override {
        return PathUtil::JoinPath(index_path_, file_name);
    }
    std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const override {
        return PathUtil::JoinPath(index_path_, file->FileName());
    }
    bool IsExternalPath() const override {
        return external_;
    }
    void SetExternal(bool v) {
        external_ = v;
    }

 private:
    std::string index_path_;
    bool external_ = false;
    std::shared_ptr<std::atomic<int32_t>> index_file_count_ =
        std::make_shared<std::atomic<int32_t>>(0);
};

}  // namespace paimon::test
