<!---
  Copyright 2024-present Alibaba Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Paimon C++

Paimon C++ is a high-performance C++ implementation of [Apache Paimon](https://paimon.apache.org). Paimon C++ aims to provide a native, high-performance and extensible implementation that allows native engines to access the Paimon datalake format with maximum efficiency.

## What's in the Paimon C++ library

* Write: Paimon append table and primary key table write (without compaction).
* Commit: Paimon append table commit. (Note: Limited support — only works for simple append-only tables; table with compaction, index, changelog, and stats are not supported.)
* Scan: Paimon append and primary key table batch and stream scan (without changelog).
* Read: Paimon append table read and primary key table with deletion vector read (raw read) and primary key table with merge on read (merge read).
* Batch read and write interface using the [Arrow Columnar In-Memory Format](https://arrow.apache.org) to increase throughput.
* IO interfaces to file system and built-in local and jindo file system implementation.
* File format interfaces to customize different format and built-in orc, parquet and lance format implementation.
* Memory pool interfaces and a default implementation.
* Thread pool executor interfaces and a default implementation.
* Compatible with Java Paimon format and communication protocol (e.g., commit message, data splits, manifests).
* Note: The current implementation only supports the x86_64 architecture.

## Write And Commit Example

The writing is divided into two stages:

1. Write records: write records in distributed tasks, generate commit messages.
2. Commit/Abort: collect all commit messages, commit them in a global node ('Coordinator', or named 'Driver', or named 'Committer'). When the commit fails for certain reason, abort unsuccessful commit via commit messages.


```c++
    std::string table_path = "/tmp/paimon/my.db/test_table/";
    WriteContextBuilder context_builder(table_path, "commit_user");
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context,
                           context_builder.AddOption(Options::TARGET_FILE_SIZE, "1024mb")
                               .AddOption(Options::FILE_SYSTEM, "local")
                               .Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> file_store_write,
                           FileStoreWrite::Create(std::move(write_context)));

    ::ArrowArray arrow_array;
    // prepare your arrow array
    // ...
    RecordBatchBuilder batch_builder(&arrow_array);
    batch_builder.SetPartition({{"col1", "20240813"}, {"col2", "23"}}).SetBucket(1);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> batch, batch_builder.Finish());
    PAIMON_RETURN_NOT_OK(file_store_write->Write(batch));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> commit_messages,
                           file_store_write->PrepareCommit());

    CommitContextBuilder commit_context_builder(table_path, "commit_user");
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                           commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                               .AddOption(Options::FILE_SYSTEM, "local")
                               .IgnoreEmptyCommit(false)
                               .Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit, FileStoreCommit::Create(std::move(commit_context)));
    PAIMON_RETURN_NOT_OK(commit->Commit(commit_messages));

```

## Scan and Read Example

The reading is divided into two stages:

1. Scan: read snapshot, parse manifests, filter target file set by statistical information, and generate query plan data splits.
2. Read: read the data files according to data splits, and perform schema evolution adjustment and predicate push-down optimization.

```c++
    std::string table_path = "/tmp/paimon/my.db/test_table/";
    ScanContextBuilder context_builder(table_path);
    // prepare predicate if needed
    std::shared_ptr<Predicate> predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                               /*field_type=*/FieldType::INT, Literal(10));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context,
                           context_builder.SetPredicate(predicate)
                               .AddOption(Options::SCAN_SNAPSHOT_ID, "2")
                               .AddOption(Options::FILE_SYSTEM, "local")
                               .Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                           TableScan::Create(std::move(scan_context)));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, table_scan->CreatePlan());

    ReadContextBuilder read_context_builder(table_path, /*schema_id=*/0);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                           read_context_builder.SetReadSchema({"f0", "f1"})
                               .SetPredicate(predicate)
                               .AddOption(Options::FILE_SYSTEM, "local")
                               .EnablePrefetch(true)
                               .Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read, TableRead::Create(std::move(read_context)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> batch_reader, table_read->CreateReader(plan->Splits()));

    while (true) {
          PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch read_batch, batch_reader->NextBatch());
          if (BatchReader::IsEofBatch(read_batch)) {
              break;
          }
          auto& [c_array, c_schema] = read_batch;
          // process the arrow array
          auto arrow_result = arrow::ImportArray(c_array.get(), c_schema.get());
    }

```

## Getting Started

## Development

### CMake

```
$ mkdir build
$ cd build
$ cmake ..
$ make
```
## Contributing

Paimon-cpp is an active open-source project and we welcome people who want to contribute or share good ideas!
Before contributing, you are encouraged to check out our [documentation](https://alibaba.github.io/paimon-cpp/).

If you have suggestions, feedback, want to report a bug or request a feature, please open an [issue](https://github.com/alibaba/paimon-cpp/issues/new).
Pull requests are also very welcome!

We value respectful and open collaboration, and appreciate everyone who helps make paimon-cpp better. Thank you for your support!

### Linting

Install the python package `pre-commit` and run once `pre-commit install`.

```
pip install pre-commit
pre-commit install
```

This will setup a git pre-commit-hook that is executed on each commit and will report the linting problems. To run all hooks on all files use `pre-commit run -a`.

### Dev Containers

We provide Dev Container configuration file templates.

To use a Dev Container as your development environment, follow the steps below, then select `Dev Containers: Reopen in Container` from VS Code's Command Palette.

```
cd .devcontainer
cp Dockerfile.template Dockerfile
cp devcontainer.json.template devcontainer.json
```

If you make improvements that could benefit all developers, please update the template files and submit a pull request.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Maintainership and Contributions

This project is maintained by a core team from the Storage Service team at Alibaba, including [lxy-9602](https://github.com/lxy-9602) (maintainer), [lucasfang](https://github.com/lucasfang), [lszskye](https://github.com/lszskye), and [zjw1111](https://github.com/zjw1111). We sincerely appreciate contributions from the community — your feedback and patches are welcome and highly valued. For any questions, feature proposals, or code reviews, please feel free to reach out to us directly.
