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
#include <memory_resource>

namespace lumina::core {

inline std::shared_ptr<std::pmr::memory_resource> RefMemoryResource(std::pmr::memory_resource* resource) noexcept
{
    return std::shared_ptr<std::pmr::memory_resource>(resource ? resource : std::pmr::get_default_resource(),
                                                      [](std::pmr::memory_resource*) {});
}

struct MemoryResourceConfig {
    // huge but thread unsafe
    std::shared_ptr<std::pmr::memory_resource> storage;
    // thread safe
    std::shared_ptr<std::pmr::memory_resource> instant;

    MemoryResourceConfig() = default;

    explicit MemoryResourceConfig(std::pmr::memory_resource* resource)
    {
        storage = RefMemoryResource(resource);
        instant = storage;
    }
};

inline MemoryResourceConfig Normalize(const MemoryResourceConfig& config) noexcept
{
    MemoryResourceConfig normalized;
    auto defaultHolder = RefMemoryResource(std::pmr::get_default_resource());
    normalized.storage = config.storage ? config.storage : defaultHolder;
    normalized.instant = config.instant ? config.instant : normalized.storage;
    return normalized;
}

inline std::pmr::memory_resource& GetStorageResource(const MemoryResourceConfig& config) noexcept
{
    return *(config.storage ? config.storage.get() : std::pmr::get_default_resource());
}

inline std::pmr::memory_resource& GetInstantResource(const MemoryResourceConfig& config) noexcept
{
    if (config.instant) {
        return *config.instant.get();
    }
    return GetStorageResource(config);
}

} // namespace lumina::core
