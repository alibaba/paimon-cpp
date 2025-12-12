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

#include <iostream>
#include <map>
#include <string>
#include <tuple>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "paimon/api.h"
#include "paimon/catalog/catalog.h"

arrow::Result<std::shared_ptr<arrow::StructArray>> PrepareData(const arrow::FieldVector& fields) {
    arrow::StringBuilder f0_builder;
    arrow::Int32Builder f1_builder;
    arrow::Int32Builder f2_builder;
    arrow::DoubleBuilder f3_builder;

    std::vector<std::tuple<std::string, int, int, double>> data = {
        {"Alice", 1, 0, 11.0}, {"Bob", 1, 1, 12.1}, {"Cathy", 1, 2, 13.2}};

    for (const auto& row : data) {
        ARROW_RETURN_NOT_OK(f0_builder.Append(std::get<0>(row)));
        ARROW_RETURN_NOT_OK(f1_builder.Append(std::get<1>(row)));
        ARROW_RETURN_NOT_OK(f2_builder.Append(std::get<2>(row)));
        ARROW_RETURN_NOT_OK(f3_builder.Append(std::get<3>(row)));
    }

    std::shared_ptr<arrow::Array> f0_array, f1_array, f2_array, f3_array;
    ARROW_RETURN_NOT_OK(f0_builder.Finish(&f0_array));
    ARROW_RETURN_NOT_OK(f1_builder.Finish(&f1_array));
    ARROW_RETURN_NOT_OK(f2_builder.Finish(&f2_array));
    ARROW_RETURN_NOT_OK(f3_builder.Finish(&f3_array));

    std::vector<std::shared_ptr<arrow::Array>> children = {f0_array, f1_array, f2_array, f3_array};
    auto struct_type = arrow::struct_(fields);
    return std::make_shared<arrow::StructArray>(struct_type, f0_array->length(), children);
}

paimon::Status Run(const std::string& root_path, const std::string& db_name,
                   const std::string& table_name) {
    std::map<std::string, std::string> options = {{paimon::Options::MANIFEST_FORMAT, "orc"},
                                                  {paimon::Options::FILE_FORMAT, "parquet"},
                                                  {paimon::Options::FILE_SYSTEM, "local"}};

    // create table
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::Catalog> catalog,
                           paimon::Catalog::Create(root_path, options));
    PAIMON_RETURN_NOT_OK(catalog->CreateDatabase(db_name, options, /*ignore_if_exists=*/false));
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()),
        arrow::field("f3", arrow::float64()),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    ::ArrowSchema arrow_schema;
    arrow::Status arrow_status = arrow::ExportSchema(*schema, &arrow_schema);
    if (!arrow_status.ok()) {
        return paimon::Status::Invalid(arrow_status.message());
    }
    PAIMON_RETURN_NOT_OK(catalog->CreateTable(paimon::Identifier(db_name, table_name),
                                              &arrow_schema,
                                              /*partition_keys=*/{},
                                              /*primary_keys=*/{}, options,
                                              /*ignore_if_exists=*/false));

    std::string table_path = root_path + "/" + db_name + ".db/" + table_name;

    std::string commit_user = "some_commit_user";
    // write
    paimon::WriteContextBuilder context_builder(table_path, commit_user);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::WriteContext> write_context,
                           context_builder.SetOptions(options).Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreWrite> writer,
                           paimon::FileStoreWrite::Create(std::move(write_context)));

    // prepare data
    auto struct_array = PrepareData(fields);
    if (!struct_array.ok()) {
        return paimon::Status::Invalid(struct_array.status().ToString());
    }
    ::ArrowArray arrow_array;
    arrow_status = arrow::ExportArray(*struct_array.ValueUnsafe(), &arrow_array);
    if (!arrow_status.ok()) {
        return paimon::Status::Invalid(arrow_status.message());
    }
    paimon::RecordBatchBuilder batch_builder(&arrow_array);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::RecordBatch> record_batch,
                           batch_builder.Finish());
    PAIMON_RETURN_NOT_OK(writer->Write(std::move(record_batch)));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<paimon::CommitMessage>> commit_message,
                           writer->PrepareCommit());

    // commit
    paimon::CommitContextBuilder commit_context_builder(table_path, commit_user);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::CommitContext> commit_context,
                           commit_context_builder.SetOptions(options).Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreCommit> committer,
                           paimon::FileStoreCommit::Create(std::move(commit_context)));
    PAIMON_RETURN_NOT_OK(committer->Commit(commit_message));

    // scan
    paimon::ScanContextBuilder scan_context_builder(table_path);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ScanContext> scan_context,
                           scan_context_builder.SetOptions(options).Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableScan> scanner,
                           paimon::TableScan::Create(std::move(scan_context)));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::Plan> plan, scanner->CreatePlan());

    // read
    paimon::ReadContextBuilder read_context_builder(table_path);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ReadContext> read_context,
                           read_context_builder.SetOptions(options).Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableRead> table_read,
                           paimon::TableRead::Create(std::move(read_context)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::BatchReader> batch_reader,
                           table_read->CreateReader(plan->Splits()));
    arrow::ArrayVector result_array_vector;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(paimon::BatchReader::ReadBatch batch, batch_reader->NextBatch());
        if (paimon::BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        auto arrow_result = arrow::ImportArray(c_array.get(), c_schema.get());
        if (!arrow_result.ok()) {
            return paimon::Status::Invalid(arrow_result.status().ToString());
        }
        auto result_array = arrow_result.ValueUnsafe();
        result_array_vector.push_back(result_array);
    }
    auto chunk_result = arrow::ChunkedArray::Make(result_array_vector);
    if (!chunk_result.ok()) {
        return paimon::Status::Invalid(chunk_result.status().ToString());
    }
    std::cout << chunk_result.ValueUnsafe()->ToString() << std::endl;
    return paimon::Status::OK();
}

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cout << "Usage: " << argv[0] << " <root_path> <database_name> <table_name>"
                  << std::endl;
        return -1;
    }
    const std::string root_path = argv[1];
    const std::string db_name = argv[2];
    const std::string table_name = argv[3];
    paimon::Status status = Run(root_path, db_name, table_name);
    if (!status.ok()) {
        std::cerr << "Failed to run example:" << status.ToString() << std::endl;
        return -1;
    }
    return 0;
}
