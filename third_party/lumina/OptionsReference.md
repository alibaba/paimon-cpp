# Options Reference (Generated)

This file is auto-generated. Do not edit manually.

**Note on Configuring Lumina Global Index in `paimon-cpp`**
When enabling the Lumina global index in `paimon-cpp`, all configuration parameters specific to Lumina **must be prefixed with `lumina.`**.

## core / builder

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `build.log_threshold` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | Threshold of builder log. |
| `distance.metric` | `FieldType::kString` | `false` | `ValidateMetric` | Distance metric. |
| `encoding.type` | `FieldType::kString` | `false` | `ValidateEncodingType` | Encoding type. |
| `extension.build.ckpt.count` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | Number of checkpoints. |
| `extension.build.ckpt.threshold` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | Threshold for triggering checkpoint. |
| `index.dimension` | `FieldType::kInt` | `true` | `ValidatePositiveInt` | Vector dimension. |
| `index.type` | `FieldType::kString` | `true` | `ValidateIndexType` | Index type. |
| `pretrain.sample_ratio` | `FieldType::kDouble` | `false` | `ValidateRatio01` | Sample ratio for pretrain. |

## core / quantizer

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `distance.metric` | `FieldType::kString` | `true` | `ValidateMetric` | Distance metric. |
| `encoding.type` | `FieldType::kString` | `false` | `ValidateEncodingType` | Encoding type. |
| `index.dimension` | `FieldType::kInt` | `true` | `ValidatePositiveInt` | Vector dimension. |

## core / search

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `search.parallel_number` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | Search parallel number. |
| `search.thread_safe_filter` | `FieldType::kBool` | `false` | `nullptr` | Thread safe filter. |
| `search.topk` | `FieldType::kInt` | `true` | `ValidatePositiveInt` | Search-time topK override. |

## core / searcher

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `index.dimension` | `FieldType::kInt` | `true` | `ValidatePositiveInt` | Index dimension. |
| `index.type` | `FieldType::kString` | `true` | `ValidateIndexType` | Index type. |

## diskann / builder

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `diskann.build.ef_construction` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | DiskANN build ef construction parameter. |
| `diskann.build.neighbor_count` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | DiskANN build neighbor count. |
| `diskann.build.reorder_layout` | `FieldType::kBool` | `false` | `nullptr` | DiskANN build reorder layout. |
| `diskann.build.slack_pruning_factor` | `FieldType::kDouble` | `false` | `ValidatePositiveDouble` | DiskANN build slack pruning factor. |
| `diskann.build.thread_count` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | DiskANN build thread count. |
| `diskann.disk_encoding.encoding.pq.m` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | PQ m, when DiskANN disk encoding type = PQ. |
| `diskann.disk_encoding.encoding.pq.max_epoch` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | PQ max epoch, when DiskANN disk encoding type = PQ. |
| `diskann.disk_encoding.encoding.pq.thread_count` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | PQ thread count, when DiskANN disk encoding type = PQ. |
| `diskann.disk_encoding.save_origin_embedding` | `FieldType::kBool` | `false` | `nullptr` | DiskANN disk encoding save origin embedding. |
| `diskann.disk_encoding.type` | `FieldType::kString` | `false` | `ValidateEncodingType` | DiskANN disk encoding type. |

## diskann / search

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `diskann.search.beam_width` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | Diskann search beam width. |
| `diskann.search.io_limit` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | Diskann search IO limit. |
| `diskann.search.list_size` | `FieldType::kInt` | `true` | `ValidatePositiveInt` | Diskann search list size. |
| `diskann.search.use_reorder_data` | `FieldType::kBool` | `false` | `nullptr` | Diskann search use reorder data flag. |

## diskann / searcher

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `diskann.search.num_nodes_to_cache` | `FieldType::kInt` | `false` | `ValidateNonNegativeInt` | DiskANN nodes to cache. |

## io / io

Note: `io.*` options apply to built-in file reader/writer implementations only.

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `index.path` | `FieldType::kString` | `true` | `nullptr` | Index path (built-in IO only). |
| `io.reader.mmap.lock_mode` | `FieldType::kString` | `false` | `ValidateMmapLockMode` | mmap lock mode (none/mlock/populate, built-in IO only). |
| `io.reader.type` | `FieldType::kString` | `false` | `ValidateReaderType` | Reader type (local/mmap, built-in IO only). |
| `io.verify_crc` | `FieldType::kBool` | `false` | `nullptr` | Verify section CRC on read (built-in IO only). |

## ivf / builder

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `ivf.build.max_epoch` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | IVF build max epoch. |
| `ivf.build.num_lists` | `FieldType::kInt` | `true` | `ValidatePositiveInt` | IVF num lists. |
| `ivf.build.thread_count` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | IVF build thread count. |

## ivf / search

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `search.nprobe` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | IVF search nprobe. |

## quantizer / quantizer

| Key | Type | Required | Validator | Description |
| --- | ---- | -------- | --------- | ----------- |
| `encoding.pq.m` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | PQ m. |
| `encoding.pq.make_zero_mean` | `FieldType::kBool` | `false` | `nullptr` | PQ make zero mean. |
| `encoding.pq.max_epoch` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | PQ max epoch. |
| `encoding.pq.thread_count` | `FieldType::kInt` | `false` | `ValidatePositiveInt` | PQ thread count. |
| `encoding.pq.use_opq` | `FieldType::kBool` | `false` | `nullptr` | Use OPQ. |
