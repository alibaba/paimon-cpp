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

set(PAIMON_GLOBAL_INDEX_SRC
    bitmap/bitmap_global_index.cpp
    bitmap/bitmap_global_index_factory.cpp
    btree/btree_file_footer.cpp
    btree/btree_global_index_factory.cpp
    btree/btree_global_indexer.cpp
    btree/btree_global_index_reader.cpp
    btree/btree_global_index_writer.cpp
    btree/btree_index_meta.cpp
    rangebitmap/range_bitmap_global_index.cpp
    rangebitmap/range_bitmap_global_index_factory.cpp)

add_paimon_lib(paimon_global_index
               SOURCES
               ${PAIMON_GLOBAL_INDEX_SRC}
               DEPENDENCIES
               paimon_shared
               paimon_file_index_shared
               STATIC_LINK_LIBS
               arrow
               fmt
               dl
               Threads::Threads
               SHARED_LINK_LIBS
               paimon_shared
               paimon_file_index_shared
               SHARED_LINK_FLAGS
               ${PAIMON_VERSION_SCRIPT_FLAGS})
