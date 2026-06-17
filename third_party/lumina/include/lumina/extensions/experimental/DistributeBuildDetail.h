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
#include <string_view>

#include <lumina/api/Extension.h>
#include <lumina/api/LuminaBuilder.h>
#include <lumina/core/Status.h>

namespace lumina::extensions { inline namespace experimental {

struct DistributeBuildStage {
    static constexpr std::string_view Name() noexcept { return "distribute_"; }
    struct Partition {
        static constexpr std::string_view Name() noexcept { return "distribute_partition_"; }
    };
    struct Build {
        static constexpr std::string_view Name() noexcept { return "distribute_build_"; }
    };
    struct Reduce {
        static constexpr std::string_view Name() noexcept { return "distribute_reduce_"; }
    };
};

template <typename Stage>
class DistributeBuildExtension;

namespace detail {

class DistributeBuildCombinedExtensionBase : public api::IBuildExtension
{
public:
    ~DistributeBuildCombinedExtensionBase() override = default;

    void SetBuilderStatusManager(std::shared_ptr<api::BuilderStatusManager> manager) noexcept override
    {
        _statusManager = std::move(manager);
    }

protected:
    core::Status RequireStatus(api::BuilderStatus expected) const noexcept;
    core::Status RequireReadyForInsert() const noexcept;
    void TransitionTo(api::BuilderStatus next) noexcept;

    std::shared_ptr<api::BuilderStatusManager> _statusManager;
};

} // namespace detail
}} // namespace lumina::extensions::experimental
