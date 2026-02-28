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

#include "paimon/core/index/index_path_factory.h"

namespace paimon::test {

class MockIndexPathFactory : public IndexPathFactory {
 public:
    std::string NewPath() const override {
        return "mock_path";
    }
    std::string ToPath(const std::string& file_name) const override {
        return file_name;
    }
    std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const override {
        return file ? file->FileName() : "mock_file";
    }
    bool IsExternalPath() const override {
        return external_;
    }
    void SetExternal(bool v) {
        external_ = v;
    }

 private:
    bool external_ = false;
};

}  // namespace paimon::test
