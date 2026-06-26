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

#include <memory>
#include <string>

#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {
class MemoryPool;
class OutputStream;
}  // namespace paimon

namespace paimon::test {
class MockFormatWriterBuilder : public WriterBuilder {
 public:
    MockFormatWriterBuilder();
    ~MockFormatWriterBuilder() override = default;

    WriterBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override;

    Result<std::unique_ptr<FormatWriter>> Build(const std::shared_ptr<OutputStream>& out,
                                                const std::string& compression) override;

 private:
    std::shared_ptr<MemoryPool> memory_pool_;
};

}  // namespace paimon::test
