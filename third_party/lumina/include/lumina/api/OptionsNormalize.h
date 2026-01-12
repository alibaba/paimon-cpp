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

#include <string>
#include <unordered_map>

#include <lumina/api/Options.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>

namespace lumina::api {

// Weakly-typed C++ entry: normalize string->string maps into typed Options.
core::Result<BuilderOptions> NormalizeBuilderOptions(const std::unordered_map<std::string, std::string>& raw);
core::Result<SearcherOptions> NormalizeSearcherOptions(const std::unordered_map<std::string, std::string>& raw);
core::Result<SearchOptions> NormalizeSearchOptions(const std::unordered_map<std::string, std::string>& raw);

} // namespace lumina::api
