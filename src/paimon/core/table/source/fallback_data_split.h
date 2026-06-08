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
#include <vector>

#include "paimon/table/source/data_split.h"

namespace paimon {
class FallbackDataSplit : public DataSplit {
 public:
    FallbackDataSplit(const std::shared_ptr<DataSplit>& split, bool is_fallback)
        : is_fallback_(is_fallback), split_(split) {}

    int32_t Bucket() const override {
        return split_->Bucket();
    }

    std::vector<SimpleDataFileMeta> GetFileList() const override {
        return split_->GetFileList();
    }

    bool IsFallback() const {
        return is_fallback_;
    }

    const std::shared_ptr<DataSplit>& GetSplit() const {
        return split_;
    }

 private:
    bool is_fallback_ = false;
    std::shared_ptr<DataSplit> split_;
};
}  // namespace paimon
