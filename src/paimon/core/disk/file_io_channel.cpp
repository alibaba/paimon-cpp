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

#include "paimon/core/disk/file_io_channel.h"

#include <iomanip>
#include <sstream>
#include <utility>

#include "paimon/common/utils/path_util.h"

namespace paimon {
std::string FileIOChannel::GenerateRandomHexString(std::mt19937* random) {
    std::uniform_int_distribution<int32_t> dist(0, 255);
    std::ostringstream hex_stream;
    hex_stream << std::hex << std::setfill('0');
    for (int32_t i = 0; i < kRandomBytesLength; ++i) {
        hex_stream << std::setw(2) << dist(*random);
    }
    return hex_stream.str();
}

FileIOChannel::ID::ID(const std::string& path) : path_(path) {}

FileIOChannel::ID::ID(const std::string& base_path, std::mt19937* random)
    : path_(PathUtil::JoinPath(base_path, GenerateRandomHexString(random) + ".channel")) {}

FileIOChannel::ID::ID(const std::string& base_path, const std::string& prefix, std::mt19937* random)
    : path_(PathUtil::JoinPath(base_path,
                               prefix + "-" + GenerateRandomHexString(random) + ".channel")) {}

const std::string& FileIOChannel::ID::GetPath() const {
    return path_;
}

bool FileIOChannel::ID::operator==(const ID& other) const {
    return path_ == other.path_;
}

bool FileIOChannel::ID::operator!=(const ID& other) const {
    return !(*this == other);
}

size_t FileIOChannel::ID::Hash::operator()(const ID& id) const {
    return std::hash<std::string>{}(id.path_);
}

FileIOChannel::Enumerator::Enumerator(const std::string& base_path, std::mt19937* random)
    : path_(base_path), name_prefix_(GenerateRandomHexString(random)) {}

FileIOChannel::ID FileIOChannel::Enumerator::Next() {
    std::ostringstream filename;
    filename << name_prefix_ << "." << std::setfill('0') << std::setw(6) << (local_counter_++)
             << ".channel";

    std::string full_path = PathUtil::JoinPath(path_, filename.str());
    return ID(full_path);
}

}  // namespace paimon
