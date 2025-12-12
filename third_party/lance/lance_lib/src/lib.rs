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

extern crate lance;
use tokio;
use std::ffi::{CStr, CString};
use std::slice;
use std::os::raw::c_char;
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use futures::StreamExt;

use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::Schema as ArrowSchema;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};

use lance_core::{
    datatypes::{Schema as LanceSchema},
};
use lance_core::cache::LanceCache;
use lance_file::{
    v2::{
        reader::{FileReader, FileReaderOptions, ReaderProjection},
        writer::{FileWriter, FileWriterOptions},
    },
    version::LanceFileVersion,
};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_io::{
    scheduler::{ScanScheduler, SchedulerConfig},
    stream::RecordBatchStream,
    utils::CachedFileSize,
    object_store::ObjectStore,
    ReadBatchParams,
};
use object_store::path::Path;


/// Set error message to error_message and return error code -1
/// Parameters:
/// - input_string: The Rust string to copy into the C buffer.
/// - error_message: A mutable pointer to a C buffer where the string should be copied.
/// - error_size: Size of the C buffer.
fn set_error_and_return(input_string: &str, error_message: *mut c_char, error_size: usize) -> i32 {
    if error_message.is_null() {
        // Handle null pointers if necessary
        return -1;
    }

    match CString::new(input_string) {
        Ok(c_string) => {
            // Get the byte representation including the null terminator
            let bytes = c_string.as_bytes_with_nul();
            let bytes_to_copy = bytes.len().min(error_size - 1); // Ensure room for null terminator
            unsafe {
                // Copy the bytes into the C buffer
                ptr::copy_nonoverlapping(bytes.as_ptr(), error_message as *mut u8, bytes_to_copy);
                // Ensure the buffer is null-terminated
                *error_message.add(bytes_to_copy) = 0;
            }
        },
        Err(_) => {
            // Handle error: Unable to create a CString (e.g., due to interior null byte)
            let err_msg = "Failed to convert to CString";
            let err_bytes = err_msg.as_bytes();
            let err_bytes_to_copy = err_bytes.len().min(error_size - 1);
            unsafe {
                ptr::copy_nonoverlapping(err_bytes.as_ptr(), error_message as *mut u8, err_bytes_to_copy);
                *error_message.add(err_bytes_to_copy) = 0;
            }
        }
    }
    -1 // Error code
}

use lazy_static::lazy_static;
lazy_static! {
    pub static ref RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");
}

async fn async_create_writer(
    c_file_path: *const c_char,
    schema_ptr: *mut FFI_ArrowSchema,
    file_writer_ptr: *mut *mut FileWriter,
    error_message: *mut c_char,
    error_size: usize,
) -> i32 {
    unsafe {
        if c_file_path.is_null() || schema_ptr.is_null() || file_writer_ptr.is_null() {
            return set_error_and_return("Null pointer passed to function for file_path or schema_ptr or file_writer_ptr", error_message, error_size);
        }

        let file_path = CStr::from_ptr(c_file_path);
        let path = match file_path.to_str() {
            Ok(path_str) => std::path::Path::new(path_str),
            Err(_) => return set_error_and_return("Invalid UTF-8 sequence in file path", error_message, error_size),
        };

        let object_writer = match ObjectStore::create_local_writer(&path).await {
            Ok(writer) => writer,
            Err(e) => return set_error_and_return(&format!("Failed to create local writer: {}", e), error_message, error_size),
        };

        let c_schema = FFI_ArrowSchema::from_raw(schema_ptr);
        let arrow_schema = match ArrowSchema::try_from(&c_schema) {
            Ok(schema) => schema,
            Err(e) => return set_error_and_return(&format!("Failed to convert FFI schema: {}", e), error_message, error_size),
        };

        let lance_schema = match LanceSchema::try_from(&arrow_schema) {
            Ok(ls) => ls,
            Err(e) => return set_error_and_return(&format!("Failed to convert Arrow schema to Lance schema: {}", e), error_message, error_size),
        };

        // TODO: add write options
        let lance_file_writer = match FileWriter::try_new(object_writer, lance_schema, FileWriterOptions {
            format_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        }) {
            Ok(writer) => writer,
            Err(e) => return set_error_and_return(&format!("Failed to create file writer: {}", e), error_message, error_size),
        };

        *file_writer_ptr = Box::into_raw(Box::new(lance_file_writer));
        0 // Success
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn create_writer(
    file_path: *const std::os::raw::c_char,
    schema_ptr: *mut FFI_ArrowSchema,
    file_writer_ptr: *mut *mut FileWriter,
    error_message: *mut c_char,
    error_size: usize
) -> i32 {
    RT.block_on(async_create_writer(
        file_path,
        schema_ptr,
        file_writer_ptr,
        error_message,
        error_size,
    ))

}

async fn async_write_c_arrow_array(
    file_writer_ptr: *mut FileWriter,
    input_array_ptr: *mut FFI_ArrowArray,
    input_schema_ptr: *mut FFI_ArrowSchema,
    error_message: *mut c_char,
    error_size: usize,
) -> i32 {
    if file_writer_ptr.is_null() || input_array_ptr.is_null() || input_schema_ptr.is_null() {
        return set_error_and_return("Null pointer passed to function for file_writer_ptr or input_array_ptr or input_schema_ptr", error_message, error_size);
    }
    unsafe {
        let arrow_array = FFI_ArrowArray::from_raw(input_array_ptr);
        let arrow_schema = FFI_ArrowSchema::from_raw(input_schema_ptr);
        let array_data = match from_ffi(arrow_array, &arrow_schema) {
            Ok(data) => data,
            Err(err) => return set_error_and_return(&format!("Failed to convert from FFI: {}", err), error_message, error_size),
        };

        let struct_array = StructArray::from(array_data);
        let record_batch = RecordBatch::from(&struct_array);

        let writer = &mut *file_writer_ptr;
        match writer.write_batch(&record_batch).await {
            Ok(_) => 0,
            Err(err) => set_error_and_return(&format!("Failed to write batch: {}", err), error_message, error_size),
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn write_c_arrow_array(
    file_writer_ptr: *mut FileWriter,
    input_array_ptr: *mut FFI_ArrowArray,
    input_schema_ptr: *mut FFI_ArrowSchema,
    error_message: *mut c_char,
    error_size: usize,
) -> i32 {
    RT.block_on(async_write_c_arrow_array(
        file_writer_ptr,
        input_array_ptr,
        input_schema_ptr,
        error_message,
        error_size,
    ))
}

#[unsafe(no_mangle)]
pub extern "C" fn writer_tell(file_writer_ptr: *mut FileWriter, tell_pos: *mut u64, error_message: *mut c_char, error_size: usize) -> i32 {
    match RT.block_on(async {
        if file_writer_ptr.is_null() || tell_pos.is_null() {
            return Err("Null pointer passed to function for file_writer_ptr or tell_pos".into());
        }        
        // Safely dereference the pointer within an unsafe block
        let result = unsafe {
            let writer = &mut *file_writer_ptr;
            writer.tell().await
        };        
        result.map_err(|e| e.to_string())
    }) {
        Ok(pos) => {
            unsafe {
                *tell_pos = pos;
            }
            0// Indicate success
        }
        Err(err_msg) => {
            return set_error_and_return(&err_msg.to_string(), error_message, error_size);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn finish_writer(file_writer_ptr: *mut FileWriter, error_message: *mut c_char, error_size: usize) -> i32 {
    match RT.block_on(async {
        if file_writer_ptr.is_null() {
            return Err("Null pointer passed to function for file_writer_ptr".into());
        }        
        // Safely dereference the pointer within an unsafe block
        let result = unsafe {
            let writer = &mut *file_writer_ptr;
            writer.finish().await
        };        
        result.map_err(|e| e.to_string())
    }) {
        Ok(_num_of_rows) => {
            0 // Indicate success
        }
        Err(err_msg) => {
            return set_error_and_return(&err_msg.to_string(), error_message, error_size);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn release_writer(file_writer_ptr: *mut FileWriter, error_message: *mut c_char, error_size: usize) -> i32 {
    if file_writer_ptr.is_null() {
        return set_error_and_return("Null pointer passed to function for file_writer_ptr", error_message, error_size);
    }

    unsafe {
        // Automatically drops the FileWriter
        let _ = Box::from_raw(file_writer_ptr);
    }
    0 // Return 0 to indicate success
}

////////////////////////////////////////////
pub struct LanceReaderAdapter {
    pub stream: Pin<Box<dyn RecordBatchStream>>,
}

impl LanceReaderAdapter {
    pub fn new(stream: Pin<Box<dyn RecordBatchStream>>) -> Self {
        LanceReaderAdapter { stream }
    }
}

async fn async_create_reader(
    c_file_path: *const c_char,
    file_reader_ptr: *mut *mut FileReader,
    error_message: *mut c_char,
    error_size: usize,
) -> i32 {
    unsafe {
        if c_file_path.is_null() || file_reader_ptr.is_null() {
            return set_error_and_return("Null pointer passed to function for file_path or file_reader_ptr", error_message, error_size);
        }
        let file_path = CStr::from_ptr(c_file_path);
        let file_path_str = match file_path.to_str() {
            Ok(path_str) => path_str,
            Err(_) => return set_error_and_return("Invalid UTF-8 sequence in file path", error_message, error_size),
        };
        let (obj_store, path) = match ObjectStore::from_uri(&file_path_str).await {
            Ok((o, p)) => (o, p),
            Err(e) => return set_error_and_return(&format!("Failed to create object store from uri: {}", e), error_message, error_size),
        };
        let config = SchedulerConfig::max_bandwidth(&obj_store);
        let scan_scheduler = ScanScheduler::new(obj_store, config);
        let parsed_path = match Path::parse(&path) {
            Ok(p) => p,
            Err(e) => return set_error_and_return(&format!("Failed to parse path: {}", e), error_message, error_size),
        };
        
        let file_scheduler = match scan_scheduler
            .open_file(&parsed_path, &CachedFileSize::unknown())
            .await {
                Ok(scheduler) => scheduler,
                Err(e) => return set_error_and_return(&format!("Failed to open file for scheduler {}", e), error_message, error_size),
            };
        // TODO: add more reader options
        let lance_file_reader = match FileReader::try_open(
            file_scheduler,
            /*base_projection*/None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        ).await {
                Ok(reader) => reader,
                Err(e) => return set_error_and_return(&format!("Failed to create lance file reader {}", e), error_message, error_size),
        };

        *file_reader_ptr = Box::into_raw(Box::new(lance_file_reader));
        0 // Success
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn create_reader(
    c_file_path: *const c_char,
    file_reader_ptr: *mut *mut FileReader,
    error_message: *mut c_char,
    error_size: usize,
) -> i32 {
    RT.block_on(async_create_reader(
        c_file_path,
        file_reader_ptr,
        error_message,
        error_size,
    ))
}

#[unsafe(no_mangle)]
pub extern "C" fn get_schema(
    file_reader_ptr: *mut FileReader,
    output_schema_ptr: *mut FFI_ArrowSchema,
    error_message: *mut c_char,
    error_size: usize,  
) -> i32 {
    if file_reader_ptr.is_null() || output_schema_ptr.is_null() {
        return set_error_and_return("Null pointer passed to function for file_reader_ptr or output_schema_ptr", error_message, error_size);
    }
    unsafe {
        let reader = &mut *file_reader_ptr;
        let schema = reader.schema();
        let arrow_schema = ArrowSchema::from(schema.as_ref());
        let c_schema = match FFI_ArrowSchema::try_from(&arrow_schema) {
            Ok(schema) => schema,
            Err(e) => return set_error_and_return(&format!("Failed to convert to FFI schema: {}", e), error_message, error_size),
        };
        std::ptr::write_unaligned(output_schema_ptr, c_schema);
        0
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn num_rows(
    file_reader_ptr: *mut FileReader,
    num_rows: *mut u64,
    error_message: *mut c_char,
    error_size: usize, 
) -> i32 {
    if file_reader_ptr.is_null() || num_rows.is_null() {
        return set_error_and_return("Null pointer passed to function for file_reader_ptr or num_rows", error_message, error_size);
    }
    unsafe {
        let reader = &mut *file_reader_ptr;
        *num_rows = reader.num_rows();
        0
    }
}

async fn async_create_stream_reader(
    file_reader_ptr: *mut FileReader,
    stream_reader_ptr : *mut *mut LanceReaderAdapter,
    batch_size : u32,
    batch_readahead : u32,
    projection_column_names: *const *const c_char,
    projection_column_count: usize,
    read_row_ids: *const u32,
    read_row_count: usize,
    error_message: *mut c_char,
    error_size: usize,
) -> i32 {
    if file_reader_ptr.is_null() || stream_reader_ptr.is_null() {
        return set_error_and_return("Null pointer passed to function for file_reader_ptr or stream_reader_ptr", error_message, error_size);
    }
    unsafe {
        let reader = &mut *file_reader_ptr;
        let projection = if projection_column_count == 0 {
            ReaderProjection::from_whole_schema(reader.schema(), reader.metadata().version())
        } else {
            if projection_column_names.is_null() {
                return set_error_and_return("Null pointer passed to function for projection_column_names, while projection_column_count > 0", error_message, error_size);
            }
            let projection_column_strings: &[*const c_char] =
            std::slice::from_raw_parts(projection_column_names, projection_column_count);
            let column_names: Vec<&str> = projection_column_strings.iter()
                .filter_map(|&s| {
                    if s.is_null() {
                        None
                    } else {
                        let c_string = CStr::from_ptr(s);
                        c_string.to_str().ok()
                    }
                })
                .collect();
            
            match ReaderProjection::from_column_names(reader.metadata().version(), reader.schema(), &column_names) {
                Ok(p) => p,
                Err(e) => return set_error_and_return(&format!("Failed to create projection {}", e), error_message, error_size), 
            }
        };
        let read_range = if read_row_count == 0 {
          lance_io::ReadBatchParams::RangeFull  
        } else {
            if read_row_ids.is_null() {
                return set_error_and_return("Null pointer passed to function for read_row_ids, while read_row_count > 0", error_message, error_size);
            }
            let indices: &[u32] = slice::from_raw_parts(read_row_ids, read_row_count);
            ReadBatchParams::from(indices)
        };
        let stream = match reader.read_stream_projected(
            read_range,
            batch_size,
            batch_readahead,
            projection,
            FilterExpression::no_filter(),
        ) {
            Ok(s) => s,
            Err(e) => return set_error_and_return(&format!("Failed to read stream {}", e), error_message, error_size),
        };
        *stream_reader_ptr = Box::into_raw(Box::new(LanceReaderAdapter{stream: stream}));
        0
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn create_stream_reader(
    file_reader_ptr: *mut FileReader,
    stream_reader_ptr : *mut *mut LanceReaderAdapter,
    batch_size : u32,
    batch_readahead : u32,
    projection_column_names: *const *const c_char,
    projection_column_count: usize,
    read_row_ids: *const u32,
    read_row_count: usize,
    error_message: *mut c_char,
    error_size: usize,
) -> i32 {
    RT.block_on(async_create_stream_reader(
        file_reader_ptr,
        stream_reader_ptr,
        batch_size,
        batch_readahead,
        projection_column_names,
        projection_column_count,
        read_row_ids,
        read_row_count,
        error_message,
        error_size,
    ))
}

#[unsafe(no_mangle)]
pub extern "C" fn next_batch(
    stream_reader_ptr: *mut LanceReaderAdapter,
    output_array_ptr: *mut FFI_ArrowArray,
    output_schema_ptr: *mut FFI_ArrowSchema,
    is_eof: *mut bool,
    error_message: *mut c_char,
    error_size: usize,
) -> i32 {
    if stream_reader_ptr.is_null() || output_array_ptr.is_null() || output_schema_ptr.is_null() || is_eof.is_null() {
        return set_error_and_return("Null pointer passed to function for stream_reader_ptr or output_array_ptr or output_schema_ptr or is_eof", error_message, error_size);
    }
    unsafe {
        let reader = &mut *stream_reader_ptr;
        match RT.block_on(reader.stream.next()) {
            Some(Ok(b)) => {
                let array = StructArray::from(b);
                let (out_array, out_schema) = match to_ffi(&array.to_data()) {
                    Ok((a, s)) => (a, s),
                    Err(e) => return set_error_and_return(&format!("Failed to convert to array & schema, {}", e), error_message, error_size),
                };        
                std::ptr::write_unaligned(output_array_ptr, out_array);
                std::ptr::write_unaligned(output_schema_ptr, out_schema);
                *is_eof = false;
                return 0
            },
            Some(Err(e)) => {
                return set_error_and_return(&format!("Failed to next batch, {}", e), error_message, error_size)
            },
            None => {
                // read eof
                *is_eof = true;
                return 0;   
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn release_reader(file_reader_ptr: *mut FileReader, error_message: *mut c_char, error_size: usize) -> i32 {
    if file_reader_ptr.is_null() {
        return set_error_and_return("Null pointer passed to function for file_reader_ptr", error_message, error_size);
    }
    unsafe {
        // Automatically drops the FileReader
        let _ = Box::from_raw(file_reader_ptr);
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn release_stream_reader(stream_reader_ptr: *mut LanceReaderAdapter, error_message: *mut c_char, error_size: usize) -> i32 {
    if stream_reader_ptr.is_null() {
        return set_error_and_return("Null pointer passed to function for stream_reader_ptr", error_message, error_size);
    }
    unsafe {
        // Automatically drops the ReaderAdapater
        let _ = Box::from_raw(stream_reader_ptr);
    }
    0
}

