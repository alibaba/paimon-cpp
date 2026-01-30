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

#include <sstream>
#include <string>

#include "avro/Node.hh"

namespace paimon::avro {

class AvroUtils {
 public:
    AvroUtils() = delete;
    ~AvroUtils() = delete;

    static std::string ToString(const ::avro::NodePtr& node) {
        std::stringstream ss;
        ss << *node;
        return ss.str();
    }

    static std::string ToString(const ::avro::LogicalType& type) {
        std::stringstream ss;
        type.printJson(ss);
        return ss.str();
    }

    static bool HasMapLogicalType(const ::avro::NodePtr& node) {
        return node->logicalType().type() == ::avro::LogicalType::CUSTOM &&
               node->logicalType().customLogicalType() != nullptr &&
               node->logicalType().customLogicalType()->name() == "map";
    }
};

}  // namespace paimon::avro
