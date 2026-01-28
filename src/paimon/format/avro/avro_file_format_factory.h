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

#pragma once

#include <map>
#include <memory>
#include <string>

#include "avro/LogicalType.hh"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/result.h"

namespace paimon::avro {

struct MapLogicalType : public ::avro::CustomLogicalType {
    MapLogicalType() : ::avro::CustomLogicalType("map") {}
};

class AvroFileFormatFactory : public FileFormatFactory {
 public:
    static const char IDENTIFIER[];

    const char* Identifier() const override {
        return IDENTIFIER;
    }

    Result<std::unique_ptr<FileFormat>> Create(
        const std::map<std::string, std::string>& options) const override;
};

}  // namespace paimon::avro
