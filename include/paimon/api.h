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

// Coarse public API while the library is in development

#pragma once

#include "paimon/commit_context.h"           // IWYU pragma: export
#include "paimon/defs.h"                     // IWYU pragma: export
#include "paimon/factories/factory.h"        // IWYU pragma: export
#include "paimon/file_store_commit.h"        // IWYU pragma: export
#include "paimon/file_store_write.h"         // IWYU pragma: export
#include "paimon/fs/file_system_factory.h"   // IWYU pragma: export
#include "paimon/global_config.h"            // IWYU pragma: export
#include "paimon/memory/memory_pool.h"       // IWYU pragma: export
#include "paimon/predicate/predicate.h"      // IWYU pragma: export
#include "paimon/read_context.h"             // IWYU pragma: export
#include "paimon/reader/batch_reader.h"      // IWYU pragma: export
#include "paimon/record_batch.h"             // IWYU pragma: export
#include "paimon/result.h"                   // IWYU pragma: export
#include "paimon/scan_context.h"             // IWYU pragma: export
#include "paimon/status.h"                   // IWYU pragma: export
#include "paimon/table/source/table_read.h"  // IWYU pragma: export
#include "paimon/table/source/table_scan.h"  // IWYU pragma: export
#include "paimon/write_context.h"            // IWYU pragma: export

/// Top-level namespace for Paimon C++ API.
namespace paimon {}
