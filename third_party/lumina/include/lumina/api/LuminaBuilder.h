/*
 * Copyright 2025-present Alibaba Inc.
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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "lumina/api/Dataset.h"
#include "lumina/api/Options.h"
#include "lumina/core/MemoryResource.h"
#include "lumina/core/NoCopyable.h"
#include "lumina/core/Status.h"

namespace lumina::io {
class FileWriter;
}

namespace lumina::api {

class IBuildExtension;
enum class BuilderStatus : uint8_t {
    Initial,
    Trained,
    TrainNotNeeded,
    DataInjected,
    Dumped,
    Error
};

class LuminaBuilder final : public core::NoCopyable
{
public:
    class Impl;
    friend class Impl;
    LuminaBuilder(LuminaBuilder&&) noexcept;
    LuminaBuilder(Impl&& impl);
    ~LuminaBuilder();
    static core::Result<LuminaBuilder> Create(const BuilderOptions& options);
    static core::Result<LuminaBuilder> Create(const BuilderOptions& options,
                                              const core::MemoryResourceConfig& memoryConfig);

    BuilderStatus GetStatus() const noexcept;

    core::Status Pretrain(const float* data, uint64_t n);

    core::Status PretrainFrom(api::Dataset& dataset);

    core::Status Attach(api::IBuildExtension& ext);

    core::Status InsertBatch(const float* data, const core::VectorId* ids, uint64_t n);

    core::Status InsertFrom(api::Dataset& dataset);

    core::Status Dump(const IOOptions& ioOptions);
    core::Status Dump(std::unique_ptr<io::FileWriter> fileWriter, const IOOptions& ioOptions);

private:
    LuminaBuilder();

    friend struct core::Result<LuminaBuilder>;
    std::unique_ptr<Impl> _p;
};

}
