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

#include <cstdint>
#include <memory>
#include <vector>

#include "paimon/reader/count_reader.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataSplitImpl;
class Executor;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
class MergeFileSplitRead;
class Split;

class PKCountReader : public CountReader {
 public:
    ~PKCountReader() override;

    static Result<std::unique_ptr<PKCountReader>> Create(
        std::vector<std::shared_ptr<Split>> splits,
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<InternalReadContext>& context,
        const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor);

    Result<int64_t> CountRows() override;

 private:
    PKCountReader(std::vector<std::shared_ptr<Split>> splits,
                  const std::shared_ptr<InternalReadContext>& context,
                  std::unique_ptr<MergeFileSplitRead>&& merge_read,
                  const std::shared_ptr<MemoryPool>& memory_pool);

    Result<int64_t> CountSingleSplit(const std::shared_ptr<Split>& split);
    Result<int64_t> MetadataCount(const std::shared_ptr<DataSplitImpl>& split);
    Result<int64_t> MergeCount(const std::shared_ptr<DataSplitImpl>& split);

 private:
    std::vector<std::shared_ptr<Split>> splits_;
    std::shared_ptr<InternalReadContext> context_;
    std::unique_ptr<MergeFileSplitRead> merge_read_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
