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

#include "paimon/utils/special_field_ids.h"

namespace paimon {
const int32_t SpecialFieldIds::CPP_FIELD_ID_END = (std::numeric_limits<int32_t>::max() - 10000);

const int32_t SpecialFieldIds::SEQUENCE_NUMBER = std::numeric_limits<int32_t>::max() - 1;

const int32_t SpecialFieldIds::VALUE_KIND = (std::numeric_limits<int32_t>::max() - 2);

const int32_t SpecialFieldIds::ROW_ID(std::numeric_limits<int32_t>::max() - 5);

const int32_t SpecialFieldIds::INDEX_SCORE = (SpecialFieldIds::CPP_FIELD_ID_END - 1);
}  // namespace paimon
