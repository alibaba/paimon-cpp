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

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <lumina/api/Dataset.h>
#include <lumina/api/Options.h>
#include <lumina/core/Status.h>
#include <lumina/core/Types.h>
#include <lumina/extensions/experimental/BuildCombinedExtensionV0.h>
#include <lumina/extensions/experimental/DistributeBuildDetail.h>
#include <lumina/io/FileReader.h>
#include <lumina/io/FileWriter.h>

namespace lumina::extensions { inline namespace experimental {

// =====================================================================
// Partition stage
// =====================================================================
template <>
class DistributeBuildExtension<DistributeBuildStage::Partition> : public detail::DistributeBuildCombinedExtensionBase
{
public:
    class Impl;

    DistributeBuildExtension();
    ~DistributeBuildExtension() override;

    static constexpr std::string_view ExtensionName() noexcept { return DistributeBuildStage::Partition::Name(); }
    std::string_view Name() const noexcept override { return ExtensionName(); }

    core::Status Pretrain(const float* data, uint64_t n) noexcept;
    core::Status PretrainFrom(api::Dataset& dataset) noexcept;
    core::Status InsertBatch(const float* data, const core::vector_id_t* ids, uint64_t n) noexcept;
    core::Status InsertFrom(api::Dataset& dataset) noexcept;
    core::Status Dump(std::vector<std::unique_ptr<io::FileWriter>> writers, const api::IOOptions& ioOptions) noexcept;

private:
    friend Impl& GetPartitionImpl(DistributeBuildExtension<DistributeBuildStage::Partition>& ext) noexcept;

    std::unique_ptr<Impl> _impl;
};

// =====================================================================
// Build stage
// =====================================================================
template <>
class DistributeBuildExtension<DistributeBuildStage::Build> : public detail::DistributeBuildCombinedExtensionBase
{
public:
    class Impl;

    DistributeBuildExtension();
    ~DistributeBuildExtension() override;

    static constexpr std::string_view ExtensionName() noexcept { return DistributeBuildStage::Build::Name(); }
    std::string_view Name() const noexcept override { return ExtensionName(); }

    core::Status Load(std::unique_ptr<io::FileReader> reader, const api::IOOptions& ioOptions) noexcept;
    core::Status Build() noexcept;
    core::Status Dump(std::unique_ptr<io::FileWriter> writer, const api::IOOptions& ioOptions) noexcept;

private:
    friend Impl& GetBuildImpl(DistributeBuildExtension<DistributeBuildStage::Build>& ext) noexcept;

    std::unique_ptr<Impl> _impl;
};

// =====================================================================
// Reduce stage
// =====================================================================
template <>
class DistributeBuildExtension<DistributeBuildStage::Reduce> : public detail::DistributeBuildCombinedExtensionBase
{
public:
    class Impl;

    DistributeBuildExtension();
    ~DistributeBuildExtension() override;

    static constexpr std::string_view ExtensionName() noexcept { return DistributeBuildStage::Reduce::Name(); }
    std::string_view Name() const noexcept override { return ExtensionName(); }

    core::Status Reduce(std::vector<std::unique_ptr<io::FileReader>> readers, std::unique_ptr<io::FileWriter> writer,
                        const api::IOOptions& ioOptions) noexcept;

private:
    friend Impl& GetReduceImpl(DistributeBuildExtension<DistributeBuildStage::Reduce>& ext) noexcept;

    std::unique_ptr<Impl> _impl;
};

// =====================================================================
// Mixin subclasses
// =====================================================================
class DistributeBuildWithCkptExtension final : public DistributeBuildExtension<DistributeBuildStage::Build>,
                                               public CkptCapabilityMixin
{
public:
    DistributeBuildWithCkptExtension() = default;
    ~DistributeBuildWithCkptExtension() override = default;

    std::string_view Name() const noexcept override
    {
        static const auto name =
            std::string(DistributeBuildStage::Build::Name()) + std::string(CkptCapabilityMixin::Name());
        return name;
    }
};

// ===== Public type aliases =====
template <typename Stage>
using DistributeBuildExtensionWithCkpt =
    std::conditional_t<std::is_same_v<Stage, DistributeBuildStage::Build>, DistributeBuildWithCkptExtension,
                       DistributeBuildExtension<Stage>>;


}} // namespace lumina::extensions::experimental
