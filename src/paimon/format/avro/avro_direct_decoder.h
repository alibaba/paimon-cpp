/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Adapted from Apache Iceberg C++
// https://github.com/apache/iceberg-cpp/blob/main/src/iceberg/avro/avro_direct_decoder_internal.h

#pragma once

#include <set>

#include "arrow/array/builder_base.h"
#include "avro/Decoder.hh"
#include "avro/Node.hh"
#include "paimon/status.h"

namespace paimon::avro {

class AvroDirectDecoder {
 public:
    /// Context for reusing scratch buffers during Avro decoding
    ///
    /// Avoids frequent small allocations by reusing temporary buffers across multiple decode
    /// operations. This is particularly important for string, binary, and decimal data types.
    struct DecodeContext {
        // Scratch buffer for string decoding (reused across rows)
        std::string string_scratch;
        // Scratch buffer for binary/decimal data (reused across rows)
        std::vector<uint8_t> bytes_scratch;
    };

    /// Directly decode Avro data to Arrow array builders without GenericDatum
    ///
    /// Eliminates the GenericDatum intermediate layer by directly calling Avro decoder
    /// methods and immediately appending to Arrow builders.
    ///
    /// @param avro_node The Avro schema node for the data being decoded
    /// @param decoder The Avro decoder positioned at the data to read
    /// @param array_builder The Arrow array builder to append decoded data to
    /// @param ctx Decode context for reusing scratch buffers
    /// @return Status indicating success, or an error status
    static Status DecodeAvroToBuilder(const ::avro::NodePtr& avro_node,
                                      const std::optional<std::set<size_t>>& projection,
                                      ::avro::Decoder* decoder, arrow::ArrayBuilder* array_builder,
                                      DecodeContext* ctx);
};

}  // namespace paimon::avro
