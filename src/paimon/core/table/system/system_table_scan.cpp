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

#include "paimon/core/table/system/system_table_scan.h"

#include <memory>
#include <vector>

#include "paimon/core/table/source/plan_impl.h"

namespace paimon {

SystemTableScan::SystemTableScan(const std::string& table_path) : table_path_(table_path) {}

Result<std::shared_ptr<Plan>> SystemTableScan::CreatePlan() {
    std::vector<std::shared_ptr<Split>> splits = {std::make_shared<SystemTableSplit>(table_path_)};
    return std::make_shared<PlanImpl>(std::nullopt, splits);
}

}  // namespace paimon
