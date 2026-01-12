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
#include <lumina/api/Dataset.h>
#include <lumina/api/Options.h>
#include <lumina/core/MemoryResource.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Status.h>
#include <memory>
#include <string>

namespace lumina::io {
class FileWriter;
}

namespace lumina::api {

class IBuildExtension;
enum class BuilderStatus : uint8_t {
    Initial,        ///< Builder just created; no training or writes yet.
    Trained,        ///< Training completed; Insert* is allowed.
    TrainNotNeeded, ///< Backend declares training is unnecessary (flat, etc.); can write directly.
    DataInjected,   ///< At least one batch has been inserted (Batch or Dataset).
    Dumped,         ///< Dump completed; builder is in teardown stage.
    Error           ///< An error occurred; the builder can no longer be used.
};

class LuminaBuilder final : public core::NoCopyable
{
public:
    class Impl;
    LuminaBuilder(LuminaBuilder&&) noexcept;
    LuminaBuilder(std::unique_ptr<Impl> impl) noexcept;
    ~LuminaBuilder();
    // Typical offline build: create a Builder by preset (e.g. "ivf", "diskann", "bruteforce", "demo")
    static core::Result<LuminaBuilder> Create(const BuilderOptions& options);
    static core::Result<LuminaBuilder> Create(const BuilderOptions& options,
                                              const core::MemoryResourceConfig& memoryConfig);

    BuilderStatus GetStatus() const noexcept;
    /**
     * Not re-entrant.
     */
    core::Status Pretrain(const float* data, uint64_t n);
    /**
     * Pull training data from Dataset.
     * Current implementation copies all batches into memory then calls Pretrain. Not re-entrant.
     */
    core::Status PretrainFrom(api::Dataset& dataset);

    core::Status Attach(api::IBuildExtension& ext);
    /** Batch insert (row-major n×dim).
     * Contiguous row-major buffer: n rows × Dim() columns. Not re-entrant.
     */
    core::Status InsertBatch(const float* data, const core::vector_id_t* ids, uint64_t n);
    /**
     * Pull data in batches from Dataset and insert. Not re-entrant.
     */
    core::Status InsertFrom(api::Dataset& dataset);

    core::Status Dump(const IOOptions& ioOptions);
    core::Status Dump(std::unique_ptr<io::FileWriter> fileWriter, const IOOptions& ioOptions);

private:
    std::unique_ptr<Impl> _p;
};

} // namespace lumina::api
