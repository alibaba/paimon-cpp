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

#include "paimon/format/avro/avro_file_format_factory.h"

#include <utility>

#include "paimon/factories/factory.h"
#include "paimon/format/avro/avro_file_format.h"

namespace paimon::avro {

const char AvroFileFormatFactory::IDENTIFIER[] = "avro";

Result<std::unique_ptr<FileFormat>> AvroFileFormatFactory::Create(
    const std::map<std::string, std::string>& options) const {
    return std::make_unique<AvroFileFormat>(options);
}

static __attribute__((constructor)) void AvroFileFormatFactoryRegisterLogicalTypes() {
    ::avro::CustomLogicalTypeRegistry::instance().registerType(
        "map", [](const std::string&) { return std::make_shared<MapLogicalType>(); });
}

REGISTER_PAIMON_FACTORY(AvroFileFormatFactory);

}  // namespace paimon::avro
