# Options Reference (Generated)

This file is auto-generated. Do not edit manually.


## core / builder

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `build.log_threshold` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Threshold of builder log. | `src/schema/OptionSchemaDefs.h` |
| `distance.metric` | `FieldType::kString` | `false` | `false` | `ValidateMetric` | Distance metric. | `src/schema/OptionSchemaDefs.h` |
| `encoding.type` | `FieldType::kString` | `false` | `false` | `ValidateEncodingType` | Encoding type. | `src/schema/OptionSchemaDefs.h` |
| `extension.build.ckpt.count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Number of checkpoints. | `src/schema/OptionSchemaDefs.h` |
| `extension.build.ckpt.threshold` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Threshold for triggering checkpoint. | `src/schema/OptionSchemaDefs.h` |
| `index.dimension` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Vector dimension. | `src/schema/OptionSchemaDefs.h` |
| `index.type` | `FieldType::kString` | `true` | `false` | `ValidateIndexType` | Index type. | `src/schema/OptionSchemaDefs.h` |
| `pretrain.sample_ratio` | `FieldType::kDouble` | `false` | `false` | `ValidateRatio01` | Sample ratio for pretrain. | `src/schema/OptionSchemaDefs.h` |

## core / quantizer

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `distance.metric` | `FieldType::kString` | `true` | `false` | `ValidateMetric` | Distance metric. | `src/schema/OptionSchemaDefs.h` |
| `encoding.type` | `FieldType::kString` | `false` | `false` | `ValidateEncodingType` | Encoding type. | `src/schema/OptionSchemaDefs.h` |
| `index.dimension` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Vector dimension. | `src/schema/OptionSchemaDefs.h` |

## core / search

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `search.parallel_number` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Search parallel number. | `src/schema/OptionSchemaDefs.h` |
| `search.thread_safe_filter` | `FieldType::kBool` | `false` | `false` | `nullptr` | Thread safe filter. | `src/schema/OptionSchemaDefs.h` |
| `search.topk` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Search-time topK override. | `src/schema/OptionSchemaDefs.h` |

## core / searcher

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `index.dimension` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Index dimension. | `src/schema/OptionSchemaDefs.h` |
| `index.type` | `FieldType::kString` | `true` | `false` | `ValidateIndexType` | Index type. | `src/schema/OptionSchemaDefs.h` |

## diskann / builder

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `diskann.build.ef_construction` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | DiskANN build ef construction parameter. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.build.neighbor_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | DiskANN build neighbor count. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.build.quantized_build` | `FieldType::kBool` | `false` | `false` | `nullptr` | DiskANN build with quantized distance. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.build.reorder_layout` | `FieldType::kBool` | `false` | `false` | `nullptr` | DiskANN build reorder layout. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.build.slack_pruning_factor` | `FieldType::kDouble` | `false` | `false` | `ValidatePositiveDouble` | DiskANN build slack pruning factor. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.build.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | DiskANN build thread count. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.disk_encoding.encoding.pq.m` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ m, when DiskANN disk encoding type = PQ. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.disk_encoding.encoding.pq.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ max epoch, when DiskANN disk encoding type = PQ. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.disk_encoding.encoding.pq.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ thread count, when DiskANN disk encoding type = PQ. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.disk_encoding.save_origin_embedding` | `FieldType::kBool` | `false` | `false` | `nullptr` | DiskANN disk encoding save origin embedding. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.disk_encoding.type` | `FieldType::kString` | `false` | `false` | `ValidateEncodingType` | DiskANN disk encoding type. | `src/impl/index/diskann/DiskANNSchema.cpp` |

## diskann / search

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `diskann.search.beam_width` | `FieldType::kInt` | `false` | `true` | `ValidatePositiveInt` | Diskann search beam width. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.search.io_limit` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Diskann search IO limit. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.search.list_size` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Diskann search list size. | `src/impl/index/diskann/DiskANNSchema.cpp` |

## diskann / searcher

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `diskann.search.num_nodes_to_cache` | `FieldType::kInt` | `false` | `false` | `ValidateNonNegativeInt` | DiskANN nodes to cache. | `src/impl/index/diskann/DiskANNSchema.cpp` |
| `diskann.search.sector_aligned_read` | `FieldType::kBool` | `false` | `false` | `nullptr` | DiskANN search sector aligned read. | `src/impl/index/diskann/DiskANNSchema.cpp` |

## io / io

Note: `io.*` options apply to built-in file reader/writer implementations only.

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `index.path` | `FieldType::kString` | `true` | `false` | `nullptr` | Index path (built-in IO only). | `src/schema/OptionSchemaDefs.h` |
| `io.reader.mmap.lock_mode` | `FieldType::kString` | `false` | `false` | `ValidateMmapLockMode` | mmap lock mode (none/mlock/populate, built-in IO only). | `src/schema/OptionSchemaDefs.h` |
| `io.reader.type` | `FieldType::kString` | `false` | `false` | `ValidateReaderType` | Reader type (local/mmap, built-in IO only). | `src/schema/OptionSchemaDefs.h` |
| `io.verify_crc` | `FieldType::kBool` | `false` | `false` | `nullptr` | Verify section CRC on read (built-in IO only). | `src/schema/OptionSchemaDefs.h` |

## ivf / builder

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `ivf.build.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | IVF build max epoch. | `src/impl/index/ivf/IvfSchema.cpp` |
| `ivf.build.num_lists` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | IVF num lists. | `src/impl/index/ivf/IvfSchema.cpp` |
| `ivf.build.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | IVF build thread count. | `src/impl/index/ivf/IvfSchema.cpp` |

## ivf / search

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `search.nprobe` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | IVF search nprobe. | `src/impl/index/ivf/IvfSchema.cpp` |

## quantizer / quantizer

| Key | Type | Required | Deprecated | Validator | Description | Source |
| --- | ---- | -------- | ---------- | --------- | ----------- | ------ |
| `encoding.pq.m` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ m. | `src/schema/quantizer/QuantizerSchemas.h` |
| `encoding.pq.make_zero_mean` | `FieldType::kBool` | `false` | `false` | `nullptr` | PQ make zero mean. | `src/schema/quantizer/QuantizerSchemas.h` |
| `encoding.pq.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ max epoch. | `src/schema/quantizer/QuantizerSchemas.h` |
| `encoding.pq.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ thread count. | `src/schema/quantizer/QuantizerSchemas.h` |
| `encoding.pq.use_opq` | `FieldType::kBool` | `false` | `false` | `nullptr` | Use OPQ. | `src/schema/quantizer/QuantizerSchemas.h` |
| `encoding.rabitq.centroid_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | RabitQ kmeans centroid count for pretrain (default 64). Larger => better fit & slower training. | `src/schema/quantizer/QuantizerSchemas.h` |
| `encoding.rabitq.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | RabitQ kmeans max epochs for pretrain (default 10). Higher => better fit & slower training. | `src/schema/quantizer/QuantizerSchemas.h` |
| `encoding.rabitq.quantized_bit_count` | `FieldType::kInt` | `false` | `false` | `ValidateIntInSet<1, 4, 5, 8, 9>` | RabitQ code bit width (default 4). Supported: 1, 4, 5, 8, 9. Larger => better accuracy & bigger records. | `src/schema/quantizer/QuantizerSchemas.h` |
| `encoding.rabitq.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | RabitQ kmeans thread count for pretrain (default 1). Controls training parallelism only. | `src/schema/quantizer/QuantizerSchemas.h` |
