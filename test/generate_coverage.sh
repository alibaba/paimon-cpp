#!/bin/bash

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

set -e

cd $(dirname "$0")/../
mkdir -p build && cd build
cmake ../ -DCMAKE_BUILD_TYPE=Debug -DPAIMON_BUILD_TESTS=ON -DPAIMON_USE_ASAN=ON -DPAIMON_GENERATE_COVERAGE=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
make -j
make test
lcov --capture --directory src/paimon --directory test --output-file coverage.info
genhtml coverage.info --output-directory coverage

ip=$(hostname -I | awk '{print $1}')
echo
echo "See coverage files at: $PWD/coverage.info"
echo "See coverage html files at: $PWD/coverage/"
echo "View code coverage at: http://$ip:8000/coverage/index.html"
python3 -m http.server
