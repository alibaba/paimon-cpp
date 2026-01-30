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
 */
#include "gtest/gtest.h"
#include "lucene++/FileUtils.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/MiscUtils.h"
#include "paimon/global_index/lucene/lucene_directory.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::lucene::test {
class LuceneInterfaceTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class TestDocIdSetIterator : public Lucene::DocIdSetIterator {
     public:
        explicit TestDocIdSetIterator(const std::vector<int32_t>& ids)
            : Lucene::DocIdSetIterator(), ids_(ids) {}

        int32_t advance(int32_t target) override {
            int32_t doc_id = nextDoc();
            while (doc_id < target) {
                doc_id = nextDoc();
            }
            return doc_id;
        }
        int32_t docID() override {
            return ids_[cursor_];
        }
        int32_t nextDoc() override {
            if (cursor_ == ids_.size()) {
                return Lucene::DocIdSetIterator::NO_MORE_DOCS;
            }
            return ids_[cursor_++];
        }

     private:
        size_t cursor_ = 0;
        std::vector<int32_t> ids_;
    };

    class TestDocIdSet : public Lucene::DocIdSet {
     public:
        explicit TestDocIdSet(const std::vector<int32_t>& ids) : DocIdSet(), ids_(ids) {}

        Lucene::DocIdSetIteratorPtr iterator() override {
            return Lucene::newLucene<TestDocIdSetIterator>(ids_);
        }
        bool isCacheable() override {
            return true;
        }

     private:
        std::vector<int32_t> ids_;
    };

    class TestFilter : public Lucene::Filter {
     public:
        explicit TestFilter(const std::vector<int32_t>& ids) : ids_(ids) {}

        Lucene::DocIdSetPtr getDocIdSet(const Lucene::IndexReaderPtr& reader) override {
            return Lucene::newLucene<TestDocIdSet>(ids_);
        }

     private:
        std::vector<int32_t> ids_;
    };
};

TEST_F(LuceneInterfaceTest, TestSimple) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    std::string index_path = dir->Str() + "/lucene_test";
    auto lucene_dir = Lucene::FSDirectory::open(LuceneUtils::StringToWstring(index_path),
                                                Lucene::NoLockFactory::getNoLockFactory());

    Lucene::IndexWriterPtr writer = Lucene::newLucene<Lucene::IndexWriter>(
        lucene_dir,
        Lucene::newLucene<Lucene::StandardAnalyzer>(Lucene::LuceneVersion::LUCENE_CURRENT),
        /*create=*/true, Lucene::IndexWriter::MaxFieldLengthLIMITED);

    Lucene::DocumentPtr doc = Lucene::newLucene<Lucene::Document>();
    auto field = Lucene::newLucene<Lucene::Field>(L"content", L"", Lucene::Field::STORE_NO,
                                                  Lucene::Field::INDEX_ANALYZED_NO_NORMS);
    auto doc_id_field = Lucene::newLucene<Lucene::Field>(
        L"id", L"", Lucene::Field::STORE_YES, Lucene::Field::INDEX_NOT_ANALYZED_NO_NORMS);

    field->setOmitTermFreqAndPositions(false);
    doc_id_field->setOmitTermFreqAndPositions(true);
    doc->add(field);
    doc->add(doc_id_field);

    auto build = [&](const std::wstring& doc_str, int32_t doc_id) {
        field->setValue(doc_str);
        doc_id_field->setValue(LuceneUtils::StringToWstring(std::to_string(doc_id)));
        writer->addDocument(doc);
    };

    build(L"This is an test document.", 0);
    build(L"This is an new document document document.", 1);
    build(L"Document document document document test.", 2);
    build(L"unordered user-defined doc id", 5);
    build(L"", 6);  // add a null doc

    writer->optimize();
    writer->close();

    // read
    Lucene::IndexReaderPtr reader = Lucene::IndexReader::open(lucene_dir, /*read_only=*/true);
    Lucene::IndexSearcherPtr searcher = Lucene::newLucene<Lucene::IndexSearcher>(reader);
    Lucene::QueryParserPtr parser = Lucene::newLucene<Lucene::QueryParser>(
        Lucene::LuceneVersion::LUCENE_CURRENT, L"content",
        Lucene::newLucene<Lucene::StandardAnalyzer>(Lucene::LuceneVersion::LUCENE_CURRENT));
    parser->setAllowLeadingWildcard(true);

    auto search = [&](const std::wstring& query_str, int32_t limit,
                      const std::optional<std::vector<int32_t>> selected_id,
                      const std::vector<int32_t>& expected_doc_id_vec,
                      const std::vector<std::wstring>& expected_doc_id_content_vec) {
        Lucene::QueryPtr query = parser->parse(query_str);
        Lucene::TopDocsPtr results;
        if (selected_id) {
            Lucene::FilterPtr lucene_filter = Lucene::newLucene<TestFilter>(selected_id.value());
            results = searcher->search(query, lucene_filter, limit);
        } else {
            results = searcher->search(query, limit);
        }
        ASSERT_EQ(expected_doc_id_vec.size(), results->scoreDocs.size());

        std::vector<int32_t> resule_doc_id_vec;
        std::vector<std::wstring> result_doc_id_content_vec;
        for (auto score_doc : results->scoreDocs) {
            Lucene::DocumentPtr result_doc = searcher->doc(score_doc->doc);
            resule_doc_id_vec.push_back(score_doc->doc);
            result_doc_id_content_vec.push_back(result_doc->get(L"id"));
        }
        ASSERT_EQ(resule_doc_id_vec, expected_doc_id_vec);
        ASSERT_EQ(result_doc_id_content_vec, expected_doc_id_content_vec);
    };

    // result is sorted by tf-idf score
    search(L"document", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({2, 1, 0}),
           std::vector<std::wstring>({L"2", L"1", L"0"}));
    search(L"document", /*limit=*/1, /*selected_id=*/std::nullopt, std::vector<int32_t>({2}),
           std::vector<std::wstring>({L"2"}));
    search(L"test AND document", /*limit=*/10, /*selected_id=*/std::nullopt,
           std::vector<int32_t>({2, 0}), std::vector<std::wstring>({L"2", L"0"}));
    search(L"test OR new", /*limit=*/10, /*selected_id=*/std::nullopt,
           std::vector<int32_t>({1, 0, 2}), std::vector<std::wstring>({L"1", L"0", L"2"}));
    search(L"\"test document\"", /*limit=*/10, /*selected_id=*/std::nullopt,
           std::vector<int32_t>({0}), std::vector<std::wstring>({L"0"}));
    search(L"unordered", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({3}),
           std::vector<std::wstring>({L"5"}));
    search(L"*orDer*", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({3}),
           std::vector<std::wstring>({L"5"}));

    // test filter
    search(L"document", /*limit=*/10, /*selected_id=*/std::vector<int32_t>({0, 1}),
           std::vector<int32_t>({1, 0}), std::vector<std::wstring>({L"1", L"0"}));
    search(L"document OR unordered", /*limit=*/10,
           /*selected_id=*/std::vector<int32_t>({0, 1, 3}), std::vector<int32_t>({3, 1, 0}),
           std::vector<std::wstring>({L"5", L"1", L"0"}));
    search(L"unordered", /*limit=*/10, /*selected_id=*/std::vector<int32_t>({0}),
           std::vector<int32_t>(), std::vector<std::wstring>());

    reader->close();
    lucene_dir->close();
}

}  // namespace paimon::lucene::test
