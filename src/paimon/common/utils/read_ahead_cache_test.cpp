/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/utils/read_ahead_cache.h"

#include <cstring>
#include <fstream>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(TestReadAheadCache, TestBasics) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string path = dir->Str() + "/data_file";
    const std::string content = "abcdefghijklmnopqrstuvwxyz";
    std::ofstream file(path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.write(content.data(), content.size());
    ASSERT_FALSE(file.fail());
    file.close();

    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", path, {}));
    ASSERT_OK_AND_ASSIGN(auto in, fs->Open(path));

    CacheConfig config(/*buffer_size_limit=*/256 * 1024 * 1024, /*range_size_limit=*/10,
                       /*hold_size_limit=*/2, /*pre_buffer_range_count=*/6);

    auto pool = GetDefaultPool();
    ReadAheadCache cache(std::move(in), config, pool);
    cache.Init({{1, 2}, {3, 2}, {8, 2}, {10, 4}, {14, 0}, {15, 4}, {20, 2}, {25, 0}});

    auto assert_slice_equal = [](const ByteSlice& slice, const std::string& expected) {
        ASSERT_TRUE(slice.buffer) << expected;
        EXPECT_EQ(expected, std::string_view(slice.buffer->data() + slice.offset, slice.length));
    };

    ByteSlice slice;

    ASSERT_FALSE(cache.Read({20, 2}).value().buffer);
    ASSERT_FALSE(cache.Read({1, 2}).value().buffer);

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({20, 2}));
    assert_slice_equal(slice, "uv");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({1, 2}));
    assert_slice_equal(slice, "bc");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({3, 2}));
    assert_slice_equal(slice, "de");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({8, 2}));
    assert_slice_equal(slice, "ij");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({10, 4}));
    assert_slice_equal(slice, "klmn");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({15, 4}));
    assert_slice_equal(slice, "pqrs");

    ASSERT_OK_AND_ASSIGN(slice, cache.Read({19, 3}));
    assert_slice_equal(slice, "tuv");

    // Zero-sized
    ASSERT_OK_AND_ASSIGN(slice, cache.Read({14, 0}));
    assert_slice_equal(slice, "");
    ASSERT_OK_AND_ASSIGN(slice, cache.Read({25, 0}));
    assert_slice_equal(slice, "");

    // Non-cached ranges

    ASSERT_FALSE(cache.Read({20, 3}).value().buffer);
    ASSERT_FALSE(cache.Read({0, 3}).value().buffer);
    ASSERT_FALSE(cache.Read({25, 2}).value().buffer);
}

}  // namespace paimon::test
