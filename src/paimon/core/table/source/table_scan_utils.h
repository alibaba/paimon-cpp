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

#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_scan.h"

namespace paimon {

Result<std::unique_ptr<TableScan>> NewDataTableScan(std::unique_ptr<ScanContext> context);
Result<std::unique_ptr<TableScan>> NewDataTableScan(const std::shared_ptr<ScanContext>& context);

}  // namespace paimon
