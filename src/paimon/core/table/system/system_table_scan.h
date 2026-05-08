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

#include <memory>
#include <string>
#include <vector>

#include "paimon/table/source/table_scan.h"

namespace paimon {
class Plan;
class Split;

class SystemTableSplit : public Split {
 public:
    explicit SystemTableSplit(const std::string& table_path) : table_path_(table_path) {}

    const std::string& TablePath() const {
        return table_path_;
    }

 private:
    std::string table_path_;
};

class SystemTableScan : public TableScan {
 public:
    explicit SystemTableScan(const std::string& table_path);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

 private:
    std::string table_path_;
};

}  // namespace paimon
