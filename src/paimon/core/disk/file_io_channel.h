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
#include <random>
#include <string>

#include "paimon/visibility.h"

namespace paimon {
class PAIMON_EXPORT FileIOChannel {
 public:
    class PAIMON_EXPORT ID {
     public:
        ID() = default;

        explicit ID(const std::string& path);

        ID(const std::string& base_path, std::mt19937* random);

        ID(const std::string& base_path, const std::string& prefix, std::mt19937* random);

        const std::string& GetPath() const;

        bool operator==(const ID& other) const;

        bool operator!=(const ID& other) const;

        struct Hash {
            size_t operator()(const ID& id) const;
        };

     private:
        std::string path_;
    };

 private:
    static constexpr int32_t kRandomBytesLength = 16;
    static std::string GenerateRandomHexString(std::mt19937* random);

 public:
    class PAIMON_EXPORT Enumerator {
     public:
        Enumerator(const std::string& base_path, std::mt19937* random);

        ID Next();

     private:
        std::string path_;
        std::string name_prefix_;
        uint64_t local_counter_{0};
    };
};

}  // namespace paimon
