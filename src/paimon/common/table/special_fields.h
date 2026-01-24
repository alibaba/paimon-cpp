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

#include <cstdint>
#include <limits>
#include <string>

#include "arrow/type_fwd.h"
#include "paimon/common/types/data_field.h"
#include "paimon/utils/special_field_ids.h"

namespace paimon {

struct SpecialFields {
    static constexpr char KEY_FIELD_PREFIX[] = "_KEY_";
    static constexpr int32_t KEY_VALUE_SPECIAL_FIELD_COUNT = 2;

    static const DataField& SequenceNumber() {
        static const DataField data_field = DataField(
            SpecialFieldIds::SEQUENCE_NUMBER, arrow::field("_SEQUENCE_NUMBER", arrow::int64()));
        return data_field;
    }

    static const DataField& ValueKind() {
        static const DataField data_field =
            DataField(SpecialFieldIds::VALUE_KIND, arrow::field("_VALUE_KIND", arrow::int8()));
        return data_field;
    }

    static const DataField& RowId() {
        static const DataField data_field =
            DataField(SpecialFieldIds::ROW_ID, arrow::field("_ROW_ID", arrow::int64()));
        return data_field;
    }

    static const DataField& IndexScore() {
        static const DataField data_field =
            DataField(SpecialFieldIds::INDEX_SCORE, arrow::field("_INDEX_SCORE", arrow::float32()));
        return data_field;
    }

    static bool IsSpecialFieldName(const std::string& field_name) {
        if (field_name == SequenceNumber().Name() || field_name == ValueKind().Name() ||
            field_name == RowId().Name() || field_name == IndexScore().Name()) {
            return true;
        }
        return false;
    }
    // TODO(xinyu.lxy): add a func to complete row-tracking fields
};

}  // namespace paimon
