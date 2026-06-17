/*
 * Copyright 2026-present Alibaba Inc.
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
 *
 * jieba-rs tokenizer (PaimonJiebaTokenizer) behavior test.
 *
 * Asserts that the FFI-exposed jieba-rs tokenizer produces the expected token
 * sequence for a curated set of inputs across modes (mp / mix / full / query).
 * We do NOT require byte-level parity with cppjieba: the two backends coexist
 * and each reads only its own index.
 *
 * `hmm` mode is tested separately: FFI must return Unsupported.
 */

#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/global_index/tantivy/tantivy_ffi_handle.h"
#include "paimon/global_index/tantivy/tantivy_ffi_status.h"

extern "C" {
#include "paimon_tantivy_ffi.h"  // NOLINT(build/include_subdir)
}

#ifndef JIEBA_TEST_DICT_DIR
#error "JIEBA_TEST_DICT_DIR must be set at compile time for this test"
#endif

namespace paimon::tantivy::test {
namespace {

/// Parse the FFI `tokenize` output (tab-separated: from\tto\tpos\ttext\n) and
/// return only the token text sequence.
std::vector<std::string> ExtractTokenTexts(const PaimonTantivyBuffer& buf) {
    std::vector<std::string> out;
    if (buf.len == 0) {
        return out;
    }
    std::string s(reinterpret_cast<const char*>(buf.data), buf.len);
    std::istringstream in(s);
    std::string row;
    while (std::getline(in, row)) {
        // extract text field = after 3rd '\t'
        size_t p1 = row.find('\t');
        if (p1 == std::string::npos) {
            continue;
        }
        size_t p2 = row.find('\t', p1 + 1);
        if (p2 == std::string::npos) {
            continue;
        }
        size_t p3 = row.find('\t', p2 + 1);
        if (p3 == std::string::npos) {
            continue;
        }
        out.emplace_back(row.substr(p3 + 1));
    }
    return out;
}

std::vector<std::string> TokenizeWithTantivy(PaimonJiebaTokenizer* tok, const std::string& text) {
    BufferGuard buf;
    PaimonTantivyStatus st =
        paimon_tantivy_tokenizer_tokenize(tok, text.data(), text.size(), buf.out());
    EXPECT_EQ(st, PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_OK)
        << "FFI tokenize failed: " << paimon_tantivy_last_error();
    return ExtractTokenTexts(*buf.out());
}

}  // namespace

TEST(TantivyTokenizer, HmmModeReturnsUnsupported) {
    std::string dict_dir = JIEBA_TEST_DICT_DIR;
    PaimonJiebaTokenizer* handle = nullptr;
    PaimonTantivyStatus st =
        paimon_tantivy_tokenizer_new("hmm", /*with_position=*/true, dict_dir.c_str(), &handle);
    ASSERT_EQ(st, PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_UNSUPPORTED);
    ASSERT_EQ(handle, nullptr);
    std::string err = paimon_tantivy_last_error();
    ASSERT_NE(err.find("hmm"), std::string::npos);
}

// ---------------- positive jieba-rs behavior assertions ----------------
//
// We do NOT require byte-level parity with cppjieba (the two backends coexist
// and each reads only its own index). Instead assert jieba-rs produces the
// expected token sequence for a curated set of inputs.

struct JiebaRsCase {
    std::string mode;
    std::string input;
    std::vector<std::string> expected;
};

class JiebaRsBehavior : public ::testing::TestWithParam<JiebaRsCase> {};

TEST_P(JiebaRsBehavior, ProducesExpectedTokens) {
    const auto& c = GetParam();
    std::string dict_dir = JIEBA_TEST_DICT_DIR;
    PaimonJiebaTokenizer* handle = nullptr;
    PaimonTantivyStatus st = paimon_tantivy_tokenizer_new(c.mode.c_str(), /*with_position=*/true,
                                                          dict_dir.c_str(), &handle);
    ASSERT_EQ(st, PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_OK) << paimon_tantivy_last_error();
    auto got = TokenizeWithTantivy(handle, c.input);
    ASSERT_EQ(got, c.expected) << "mode=" << c.mode << " input=" << c.input;
    paimon_tantivy_tokenizer_free(handle);
}

INSTANTIATE_TEST_SUITE_P(
    BasicCases, JiebaRsBehavior,
    ::testing::Values(JiebaRsCase{"mix", "Hello World", {"hello", "world"}},
                      JiebaRsCase{"mix", "HELLO", {"hello"}},
                      JiebaRsCase{"mix", "中国人民", {"中国", "人民"}},
                      // the two single-char stop words in the input are in
                      // stop_words.utf8, so Normalize drops them from the output
                      JiebaRsCase{"mix", "他来到了网易杭研大厦", {"来到", "网易", "杭研", "大厦"}},
                      JiebaRsCase{"full", "中国", {"中", "中国", "国"}},
                      JiebaRsCase{"query", "中国人民", {"中国", "人民"}}));

}  // namespace paimon::tantivy::test
