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
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <lumina/api/Options.h>
#include <lumina/api/Query.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Status.h>

namespace lumina::api {

// Note: Attach only registers capabilities; the framework does not own extension lifetimes.
// Callers must ensure thread safety and lifetime covers the search/build process.
class ISearchExtension : public core::NoCopyable
{
public:
    virtual ~ISearchExtension() = default;
    virtual std::string_view Name() const noexcept = 0;
};

class IBuildExtension : public core::NoCopyable
{
public:
    virtual ~IBuildExtension() = default;
    virtual std::string_view Name() const noexcept = 0;
};

} // namespace lumina::api
