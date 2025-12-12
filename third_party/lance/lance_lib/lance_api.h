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
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "arrow/c/helpers.h"
extern "C" {
typedef struct FileWriter LanceFileWriter;
typedef struct FileReader LanceFileReader;
typedef struct LanceReaderAdapter LanceReaderAdapter;

int32_t create_writer(const char* file_path, ArrowSchema* schema_ptr,
                      LanceFileWriter** file_writer_ptr, char* error_message, uintptr_t error_size);

int32_t write_c_arrow_array(LanceFileWriter* file_writer_ptr, ArrowArray* input_array_ptr,
                            ArrowSchema* input_schema_ptr, char* error_message,
                            uintptr_t error_size);

int32_t writer_tell(LanceFileWriter* file_writer_ptr, uint64_t* tell_pos, char* error_message,
                    uintptr_t error_size);

int32_t finish_writer(LanceFileWriter* file_writer_ptr, char* error_message, uintptr_t error_size);

int32_t release_writer(LanceFileWriter* file_writer_ptr, char* error_message, uintptr_t error_size);

int32_t create_reader(const char* c_file_path, LanceFileReader** file_reader_ptr,
                      char* error_message, uintptr_t error_size);

int32_t get_schema(LanceFileReader* file_reader_ptr, ArrowSchema* output_schema_ptr,
                   char* error_message, uintptr_t error_size);

int32_t num_rows(LanceFileReader* file_reader_ptr, uint64_t* num_rows, char* error_message,
                 uintptr_t error_size);

int32_t create_stream_reader(LanceFileReader* file_reader_ptr,
                             LanceReaderAdapter** reader_adapter_ptr, uint32_t batch_size,
                             uint32_t batch_readahead, const char* const* projection_column_names,
                             uintptr_t projection_column_count, const uint32_t* read_row_ids,
                             uintptr_t read_row_count, char* error_message, uintptr_t error_size);

int32_t next_batch(LanceReaderAdapter* reader_adapter_ptr, ArrowArray* output_array_ptr,
                   ArrowSchema* output_schema_ptr, bool* is_eof, char* error_message,
                   uintptr_t error_size);

int32_t release_reader(LanceFileReader* file_reader_ptr, char* error_message, uintptr_t error_size);

int32_t release_stream_reader(LanceReaderAdapter* file_reader_ptr, char* error_message,
                              uintptr_t error_size);
}
