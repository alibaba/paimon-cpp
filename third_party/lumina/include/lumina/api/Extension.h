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
#include <functional>
#include <lumina/api/Options.h>
#include <lumina/api/Query.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Status.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace lumina::api {

// Note: Attach only registers capabilities; the framework does not own extension lifetimes.
// Callers must ensure thread safety and lifetime covers the search/build process.
class ISearchExtension : public core::NoCopyable, public core::NoMoveable
{
public:
    virtual ~ISearchExtension() = default;
    virtual std::string_view Name() const noexcept = 0;
};

class BuilderStatusManager;

class IBuildExtension : public core::NoCopyable, public core::NoMoveable
{
public:
    virtual ~IBuildExtension() = default;
    virtual std::string_view Name() const noexcept = 0;

    /// Called by Impl::Attach() to inject the status manager that the extension
    /// uses to coordinate state transitions (e.g., tag insert).
    /// Default implementation is a no-op for extensions that don't need it.
    virtual void SetBuilderStatusManager(std::shared_ptr<BuilderStatusManager> /*manager*/) noexcept {}
};

class IStreamExtension : public core::NoCopyable, public core::NoMoveable
{
public:
    virtual ~IStreamExtension() = default;
    virtual std::string_view Name() const noexcept = 0;
};

} // namespace lumina::api
