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

#include <functional>
#include <memory>
#include <vector>

#include <lumina/api/Extension.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Status.h>
#include <lumina/core/Types.h>
#include <lumina/extensions/experimental/CkptManager.h>
#include <lumina/extensions/experimental/DatasetWithTag.h>

namespace lumina::extensions { inline namespace experimental {

class InnerCkptManager;

using CkptWireFunc = std::function<core::Status(std::unique_ptr<InnerCkptManager>)>;
using TagReserveFunc = std::function<core::Status(uint64_t n)>;
using TagAppendBatchFunc =
    std::function<core::Status(const float* vectors, const core::vector_id_t* ids, uint64_t count,
                               const std::vector<TagDimensionData>& tagDimensionsData)>;

struct TagWiring {
    core::dimension_t dimension;
    TagReserveFunc reserveFunc;
    TagAppendBatchFunc appendBatchFunc;
};

class CkptCapabilityMixin
{
public:
    class Impl;

    static constexpr std::string_view Name() noexcept { return "checkpoint"; }

    CkptCapabilityMixin();
    ~CkptCapabilityMixin();

    core::Status LoadCkptManager(std::unique_ptr<CkptManager> manager) noexcept;

    // -- Internal: called by backend wiring, not for direct use --
    void Wire(CkptWireFunc func) noexcept;

private:
    std::unique_ptr<Impl> _ckptImpl;
};

class TagCapabilityMixin
{
public:
    class Impl;

    static constexpr std::string_view Name() noexcept { return "tag"; }

    TagCapabilityMixin();
    ~TagCapabilityMixin();

    core::Status InsertBatchWithTag(const float* data, const core::vector_id_t* ids, uint64_t n,
                                    const std::vector<TagDimensionData>& tagDimensionsData) noexcept;
    core::Status InsertFromWithTag(DatasetWithTag& dataset) noexcept;

    // -- Internal: called by backend wiring, not for direct use --
    void Wire(TagWiring wiring) noexcept;

protected:
    std::shared_ptr<api::BuilderStatusManager> _statusManager;

private:
    std::unique_ptr<Impl> _tagImpl;
};

class BuildWithCheckpointExtension final : public api::IBuildExtension, public CkptCapabilityMixin
{
public:
    BuildWithCheckpointExtension() = default;
    ~BuildWithCheckpointExtension() override = default;

    static constexpr std::string_view ExtensionName() noexcept { return CkptCapabilityMixin::Name(); }
    std::string_view Name() const noexcept override { return ExtensionName(); }
};

class BuildWithTagExtension final : public api::IBuildExtension, public TagCapabilityMixin
{
public:
    BuildWithTagExtension() = default;
    ~BuildWithTagExtension() override = default;

    static constexpr std::string_view ExtensionName() noexcept { return TagCapabilityMixin::Name(); }
    std::string_view Name() const noexcept override { return ExtensionName(); }

    void SetBuilderStatusManager(std::shared_ptr<api::BuilderStatusManager> manager) noexcept override
    {
        TagCapabilityMixin::_statusManager = std::move(manager);
    }
};

class BuildWithCkptAndTagExtension final : public api::IBuildExtension,
                                           public CkptCapabilityMixin,
                                           public TagCapabilityMixin
{
public:
    BuildWithCkptAndTagExtension() = default;
    ~BuildWithCkptAndTagExtension() override = default;

    static std::string_view ExtensionName() noexcept
    {
        static const auto name = std::string(CkptCapabilityMixin::Name()) + std::string(TagCapabilityMixin::Name());
        return name;
    }
    std::string_view Name() const noexcept override { return ExtensionName(); }

    void SetBuilderStatusManager(std::shared_ptr<api::BuilderStatusManager> manager) noexcept override
    {
        TagCapabilityMixin::_statusManager = std::move(manager);
    }
};

}} // namespace lumina::extensions::experimental
