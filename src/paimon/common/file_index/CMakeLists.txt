# Copyright 2024-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(PAIMON_FILE_INDEX_SRC
    bitmap/bitmap_file_index_factory.cpp
    bitmap/bitmap_file_index.cpp
    bitmap/bitmap_file_index_meta.cpp
    bitmap/bitmap_file_index_meta_v1.cpp
    bitmap/bitmap_file_index_meta_v2.cpp
    bsi/bit_slice_index_bitmap_file_index.cpp
    bsi/bit_slice_index_bitmap_file_index_factory.cpp
    bsi/bit_slice_index_roaring_bitmap.cpp
    bloomfilter/bloom_filter_file_index.cpp
    bloomfilter/bloom_filter_file_index_factory.cpp
    bloomfilter/fast_hash.cpp
    rangebitmap/range_bitmap_file_index.cpp
    rangebitmap/range_bitmap_file_index_factory.cpp
    rangebitmap/range_bitmap.cpp
    rangebitmap/bit_slice_index_bitmap.cpp
    rangebitmap/dictionary/chunked_dictionary.cpp
    rangebitmap/dictionary/fixed_length_chunk.cpp
    rangebitmap/dictionary/key_factory.cpp
    rangebitmap/utils/literal_serialization_utils.cpp)

add_paimon_lib(paimon_file_index
               SOURCES
               ${PAIMON_FILE_INDEX_SRC}
               DEPENDENCIES
               paimon_shared
               STATIC_LINK_LIBS
               arrow
               fmt
               xxhash
               dl
               Threads::Threads
               SHARED_LINK_LIBS
               paimon_shared
               SHARED_LINK_FLAGS
               ${PAIMON_VERSION_SCRIPT_FLAGS})
