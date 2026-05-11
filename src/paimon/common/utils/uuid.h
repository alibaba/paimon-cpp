/*
 * Copyright 2024-present Alibaba Inc.
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

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Adapted from RocksDB
// https://github.com/facebook/rocksdb/blob/main/port/port_posix.cc

#pragma once

#include <fstream>
#include <string>

#if defined(__APPLE__)
#include <uuid/uuid.h>

#include <array>
#endif

namespace paimon {

class UUID {
 public:
    static bool Generate(std::string* output) {
#if defined(__APPLE__)
        output->clear();
        std::array<char, 37> buffer{};
        uuid_t uuid;
        uuid_generate_random(uuid);
        uuid_unparse_lower(uuid, buffer.data());
        *output = buffer.data();
        if (output->size() == 36) {
            return true;
        }
#else
        output->clear();
        std::ifstream f("/proc/sys/kernel/random/uuid");
        if (std::getline(f, /*&*/ *output) && output->size() == 36) {
            return true;
        }
#endif
        output->clear();
        return false;
    }
};

}  // namespace paimon
