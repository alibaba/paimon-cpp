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

# Lumina third-party libraries configuration

# Set the library directory
set(LUMINA_LIB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lib)

# lumina::interface
add_library(lumina::interface SHARED IMPORTED GLOBAL)
set_target_properties(lumina::interface
                      PROPERTIES IMPORTED_LOCATION ${LUMINA_LIB_DIR}/liblumina.so
                                 IMPORTED_NO_SONAME TRUE)
