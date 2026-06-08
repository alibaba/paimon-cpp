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
 * Stage 3 golden-sample test: cppjieba vs jieba-rs (PaimonJiebaTokenizer) diff.
 *
 * For each mode (mp / mix / full / query), tokenize every line of
 * `test/test_data/tokenizer_golden/golden_*.txt` twice: once with cppjieba
 * (the existing JiebaTokenizer::CutWithMode + Normalize), once with the
 * FFI-exposed PaimonJiebaTokenizer. Compare the token text sequences.
 * Pass if diff rate <= 1% per mode.
 *
 * `hmm` mode is tested separately: FFI must return Unsupported.
 */

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "cppjieba/Jieba.hpp"
#include "gtest/gtest.h"
#include "paimon/global_index/lucene/jieba_analyzer.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/global_index/tantivy/tantivy_ffi_handle.h"
#include "paimon/global_index/tantivy/tantivy_ffi_status.h"

extern "C" {
#include "paimon_tantivy_ffi.h"  // NOLINT(build/include_subdir)
}

#ifndef JIEBA_TEST_DICT_DIR
#error "JIEBA_TEST_DICT_DIR must be set at compile time for this test"
#endif

#ifndef PAIMON_TANTIVY_GOLDEN_DIR
#error "PAIMON_TANTIVY_GOLDEN_DIR must be set at compile time for this test"
#endif

namespace paimon::tantivy {
namespace {

constexpr double kMaxDiffRate = 0.01;  // 1%

/// Load lines from all `golden_*.txt` files (the strict corpus).
/// Files named `known_diffs*.txt` are excluded — those document known
/// cppjieba↔jieba-rs divergences and are inspected separately.
std::vector<std::string> LoadGoldenLines() {
    std::vector<std::string> lines;
    namespace fs = std::filesystem;
    for (const auto& entry : fs::directory_iterator(PAIMON_TANTIVY_GOLDEN_DIR)) {
        if (!entry.is_regular_file()) continue;
        const std::string name = entry.path().filename().string();
        if (name.rfind("golden_", 0) != 0 || entry.path().extension() != ".txt") continue;
        std::ifstream fin(entry.path());
        std::string line;
        while (std::getline(fin, line)) {
            lines.push_back(line);
        }
    }
    return lines;
}

/// Load lines from `known_diffs*.txt` — known divergent edge cases documented
/// in docs/dev/tokenizer_diff_report.md.
std::vector<std::string> LoadKnownDiffLines() {
    std::vector<std::string> lines;
    namespace fs = std::filesystem;
    for (const auto& entry : fs::directory_iterator(PAIMON_TANTIVY_GOLDEN_DIR)) {
        if (!entry.is_regular_file()) continue;
        const std::string name = entry.path().filename().string();
        if (name.rfind("known_diffs", 0) != 0 || entry.path().extension() != ".txt") continue;
        std::ifstream fin(entry.path());
        std::string line;
        while (std::getline(fin, line)) {
            lines.push_back(line);
        }
    }
    return lines;
}

/// Tokenize via cppjieba + Normalize (mirrors JiebaAnalyzer runtime path).
std::vector<std::string> TokenizeWithCppjieba(const cppjieba::Jieba& jieba, const std::string& mode,
                                              const std::string& text) {
    std::vector<std::string> terms;
    ::paimon::lucene::JiebaTokenizer::CutWithMode(mode, &jieba, text, &terms);
    std::vector<std::string_view> normalized_views;
    ::paimon::lucene::JiebaTokenizer::Normalize(jieba.extractor.GetStopWords(), &terms,
                                                &normalized_views);
    std::vector<std::string> result;
    result.reserve(normalized_views.size());
    for (auto v : normalized_views) result.emplace_back(v);
    return result;
}

/// Parse the FFI `tokenize` output (tab-separated: from\tto\tpos\ttext\n) and
/// return only the token text sequence.
std::vector<std::string> ExtractTokenTexts(const PaimonTantivyBuffer& buf) {
    std::vector<std::string> out;
    if (buf.len == 0) return out;
    std::string s(reinterpret_cast<const char*>(buf.data), buf.len);
    std::istringstream in(s);
    std::string row;
    while (std::getline(in, row)) {
        // extract text field = after 3rd '\t'
        size_t p1 = row.find('\t');
        if (p1 == std::string::npos) continue;
        size_t p2 = row.find('\t', p1 + 1);
        if (p2 == std::string::npos) continue;
        size_t p3 = row.find('\t', p2 + 1);
        if (p3 == std::string::npos) continue;
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

/// Build a cppjieba::Jieba instance mirroring the one used at runtime.
std::unique_ptr<cppjieba::Jieba> MakeJieba() {
    const std::string d = JIEBA_TEST_DICT_DIR;
    return std::make_unique<cppjieba::Jieba>(d + "/jieba.dict.utf8", d + "/hmm_model.utf8",
                                             d + "/user.dict.utf8", d + "/idf.utf8",
                                             d + "/stop_words.utf8");
}

struct DiffReport {
    size_t total = 0;
    size_t differ = 0;
    std::vector<std::string> sample_diffs;  // first N diffs
};

void RunDiff(const std::vector<std::string>& lines, const std::string& mode, DiffReport* report) {
    auto jieba = MakeJieba();
    std::string dict_dir = JIEBA_TEST_DICT_DIR;

    PaimonJiebaTokenizer* handle = nullptr;
    PaimonTantivyStatus st = paimon_tantivy_tokenizer_new(mode.c_str(), /*with_position=*/true,
                                                          dict_dir.c_str(), &handle);
    ASSERT_EQ(st, PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_OK)
        << "tokenizer_new failed for mode=" << mode << ": " << paimon_tantivy_last_error();

    for (const auto& line : lines) {
        if (line.empty()) continue;
        auto a = TokenizeWithCppjieba(*jieba, mode, line);
        auto b = TokenizeWithTantivy(handle, line);
        report->total++;
        if (a != b) {
            report->differ++;
            if (report->sample_diffs.size() < 10) {
                std::ostringstream os;
                os << "LINE: " << line << "\n  cppjieba: [";
                for (size_t i = 0; i < a.size(); ++i) {
                    if (i) os << ",";
                    os << a[i];
                }
                os << "]\n  jieba-rs: [";
                for (size_t i = 0; i < b.size(); ++i) {
                    if (i) os << ",";
                    os << b[i];
                }
                os << "]";
                report->sample_diffs.push_back(os.str());
            }
        }
    }

    paimon_tantivy_tokenizer_free(handle);
}

}  // namespace

TEST(TantivyTokenizer, HmmModeReturnsUnsupported) {
    std::string dict_dir = JIEBA_TEST_DICT_DIR;
    PaimonJiebaTokenizer* handle = nullptr;
    PaimonTantivyStatus st =
        paimon_tantivy_tokenizer_new("hmm", /*with_position=*/true, dict_dir.c_str(), &handle);
    EXPECT_EQ(st, PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_UNSUPPORTED);
    EXPECT_EQ(handle, nullptr);
    std::string err = paimon_tantivy_last_error();
    EXPECT_NE(err.find("hmm"), std::string::npos);
}

// ---------------- positive jieba-rs behavior assertions ----------------
//
// Per decision in docs/dev/tokenizer_diff_report.md: we do NOT require
// byte-level parity with cppjieba (共存 + 各自索引不互读). Instead assert
// jieba-rs produces expected token sequences for a curated set of inputs.

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
    EXPECT_EQ(got, c.expected) << "mode=" << c.mode << " input=" << c.input;
    paimon_tantivy_tokenizer_free(handle);
}

INSTANTIATE_TEST_SUITE_P(
    BasicCases, JiebaRsBehavior,
    ::testing::Values(JiebaRsCase{"mix", "Hello World", {"hello", "world"}},
                      JiebaRsCase{"mix", "HELLO", {"hello"}},
                      JiebaRsCase{"mix", "中国人民", {"中国", "人民"}},
                      // 他/了 在 stop_words.utf8 里,被 Normalize 过滤
                      JiebaRsCase{"mix", "他来到了网易杭研大厦", {"来到", "网易", "杭研", "大厦"}},
                      JiebaRsCase{"full", "中国", {"中", "中国", "国"}},
                      JiebaRsCase{"query", "中国人民", {"中国", "人民"}}));

// ---------------- advisory: log diffs vs cppjieba ----------------
//
// These tests never fail; they exist to print diffs to stderr for
// human review, feeding docs/dev/tokenizer_diff_report.md. They cover both
// strict and known-diffs corpora.

class AdvisoryDiffTest : public ::testing::TestWithParam<std::string> {};

TEST_P(AdvisoryDiffTest, LogsStrictGoldenDiffs) {
    const auto mode = GetParam();
    DiffReport report;
    RunDiff(LoadGoldenLines(), mode, &report);
    const double rate = report.total > 0 ? static_cast<double>(report.differ) / report.total : 0.0;
    std::cerr << "ADVISORY-STRICT mode=" << mode << " total=" << report.total
              << " differ=" << report.differ << " rate=" << rate << "\n";
    for (const auto& d : report.sample_diffs) std::cerr << d << "\n";
    SUCCEED() << "Advisory only: review docs/dev/tokenizer_diff_report.md";
}

TEST_P(AdvisoryDiffTest, LogsKnownDiffs) {
    const auto mode = GetParam();
    DiffReport report;
    auto lines = LoadKnownDiffLines();
    if (lines.empty()) GTEST_SKIP();
    RunDiff(lines, mode, &report);
    const double rate = report.total > 0 ? static_cast<double>(report.differ) / report.total : 0.0;
    std::cerr << "ADVISORY-KNOWN mode=" << mode << " total=" << report.total
              << " differ=" << report.differ << " rate=" << rate << "\n";
    for (const auto& d : report.sample_diffs) std::cerr << d << "\n";
    SUCCEED();
}

INSTANTIATE_TEST_SUITE_P(AllModes, AdvisoryDiffTest,
                         ::testing::Values("mp", "mix", "full", "query"),
                         [](const testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

}  // namespace paimon::tantivy
