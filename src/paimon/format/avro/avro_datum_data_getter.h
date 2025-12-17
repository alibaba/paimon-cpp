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
#include <vector>

#include "avro/GenericDatum.hh"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/format/avro/avro_array_data_getter.h"
#include "paimon/format/avro/avro_record_data_getter.h"

namespace paimon::avro {

class AvroDatumDataGetter {
 public:
    static bool IsNullAt(const ::avro::GenericDatum& datum) {
        return (datum.type() == ::avro::AVRO_NULL);
    }
    static bool GetBoolean(const ::avro::GenericDatum& datum) {
        assert(datum.type() == ::avro::AVRO_BOOL);
        return datum.value<bool>();
    }
    static char GetByte(const ::avro::GenericDatum& datum) {
        assert(datum.type() == ::avro::AVRO_INT);
        return datum.value<int32_t>();
    }
    static int16_t GetShort(const ::avro::GenericDatum& datum) {
        assert(datum.type() == ::avro::AVRO_INT);
        return datum.value<int32_t>();
    }
    static int32_t GetInt(const ::avro::GenericDatum& datum) {
        assert(datum.type() == ::avro::AVRO_INT);
        return datum.value<int32_t>();
    }
    static int32_t GetDate(const ::avro::GenericDatum& datum) {
        assert(datum.type() == ::avro::AVRO_INT);
        return datum.value<int32_t>();
    }
    static int64_t GetLong(const ::avro::GenericDatum& datum) {
        assert(datum.type() == ::avro::AVRO_LONG);
        return datum.value<int64_t>();
    }
    static float GetFloat(const ::avro::GenericDatum& datum) {
        assert(datum.type() == ::avro::AVRO_FLOAT);
        return datum.value<float>();
    }
    static double GetDouble(const ::avro::GenericDatum& datum) {
        assert(datum.type() == ::avro::AVRO_DOUBLE);
        return datum.value<double>();
    }
    static BinaryString GetString(const ::avro::GenericDatum& datum,
                                  const std::shared_ptr<MemoryPool>& pool) {
        assert(datum.type() == ::avro::AVRO_STRING);
        return BinaryString::FromString(datum.value<std::string>(), pool.get());
    }
    static std::string_view GetStringView(const ::avro::GenericDatum& datum) {
        if (datum.type() == ::avro::AVRO_STRING) {
            return {datum.value<std::string>()};
        } else if (datum.type() == ::avro::AVRO_BYTES) {
            const auto& binary = datum.value<std::vector<uint8_t>>();
            return {reinterpret_cast<const char*>(binary.data()), binary.size()};
        } else {
            assert(false);
            return {""};
        }
    }
    static Decimal GetDecimal(const ::avro::GenericDatum& datum, int32_t precision, int32_t scale,
                              const std::shared_ptr<MemoryPool>& pool) {
        assert(datum.type() == ::avro::AVRO_BYTES);
        auto logical_type = datum.logicalType();
        switch (logical_type.type()) {
            case ::avro::LogicalType::DECIMAL: {
                auto bytes = GetBinary(datum, pool);
                assert(logical_type.precision() == precision && logical_type.scale() == scale);
                return Decimal::FromUnscaledBytes(precision, scale, bytes.get());
            }
            default:
                assert(false);
                return Decimal::FromUnscaledLong(0, 0, 0);
        }
    }
    static Timestamp GetTimestamp(const ::avro::GenericDatum& datum, int32_t precision) {
        assert(datum.type() == ::avro::AVRO_LONG);
        switch (datum.logicalType().type()) {
            case ::avro::LogicalType::TIMESTAMP_MILLIS:
            case ::avro::LogicalType::LOCAL_TIMESTAMP_MILLIS:
                return Timestamp(/*millisecond=*/datum.value<int64_t>(), /*nano_of_millisecond=*/0);
            case ::avro::LogicalType::TIMESTAMP_MICROS:
            case ::avro::LogicalType::LOCAL_TIMESTAMP_MICROS: {
                auto [milliseconds, nanoseconds] = DateTimeUtils::TimestampConverter(
                    datum.value<int64_t>(), DateTimeUtils::MICROSECOND, DateTimeUtils::MILLISECOND,
                    DateTimeUtils::NANOSECOND);
                return Timestamp(milliseconds, nanoseconds);
            }
            case ::avro::LogicalType::TIMESTAMP_NANOS:
            case ::avro::LogicalType::LOCAL_TIMESTAMP_NANOS: {
                assert(false);  // Java Avro do not support TIMESTAMP_NANOS, should not call this
                auto [milliseconds, nanoseconds] = DateTimeUtils::TimestampConverter(
                    datum.value<int64_t>(), DateTimeUtils::NANOSECOND, DateTimeUtils::MILLISECOND,
                    DateTimeUtils::NANOSECOND);
                return Timestamp(milliseconds, nanoseconds);
            }
            default:
                assert(false);  // do not have TIMESTAMP_SECONDS/LOCAL_TIMESTAMP_SECONDS
                return Timestamp(/*millisecond=*/0, /*nano_of_millisecond=*/0);
        }
    }

    static std::shared_ptr<Bytes> GetBinary(const ::avro::GenericDatum& datum,
                                            const std::shared_ptr<MemoryPool>& pool) {
        assert(datum.type() == ::avro::AVRO_BYTES);
        const auto& binary = datum.value<std::vector<uint8_t>>();
        return std::make_shared<Bytes>(
            std::string(reinterpret_cast<const char*>(binary.data()), binary.size()), pool.get());
    }
    static std::shared_ptr<InternalArray> GetArray(const ::avro::GenericDatum& datum,
                                                   const std::shared_ptr<MemoryPool>& pool) {
        assert(datum.type() == ::avro::AVRO_ARRAY);
        return std::make_shared<AvroArrayDataGetter>(datum.value<::avro::GenericArray>(), pool);
    }
    static std::shared_ptr<InternalRow> GetRow(const ::avro::GenericDatum& datum,
                                               int32_t num_fields,
                                               const std::shared_ptr<MemoryPool>& pool) {
        assert(datum.type() == ::avro::AVRO_RECORD);
        const auto& record = datum.value<::avro::GenericRecord>();
        assert(record.fieldCount() == static_cast<size_t>(num_fields));
        return std::make_shared<AvroRecordDataGetter>(record, pool);
    }
};

}  // namespace paimon::avro
