# Options Reference (Generated)

This file is auto-generated. Do not edit manually.


## core / builder

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `build.log_threshold` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Threshold of builder log. |
| `distance.metric` | `FieldType::kString` | `false` | `false` | `ValidateMetric` | Distance metric. |
| `encoding.type` | `FieldType::kString` | `false` | `false` | `ValidateEncodingType` | Encoding type. |
| `index.dimension` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Vector dimension. |
| `index.enable_experimental` | `FieldType::kBool` | `false` | `false` | `nullptr` | Enable experimental index types. |
| `index.type` | `FieldType::kString` | `true` | `false` | `ValidateIndexType` | Index type. |
| `pretrain.sample_ratio` | `FieldType::kDouble` | `false` | `false` | `ValidateRatio01` | Sample ratio for pretrain. |

## core / searcher

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `index.dimension` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Index dimension. |
| `index.enable_experimental` | `FieldType::kBool` | `false` | `false` | `nullptr` | Enable experimental index types. |
| `index.type` | `FieldType::kString` | `true` | `false` | `ValidateIndexType` | Index type. |

## core / streamer

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `distance.metric` | `FieldType::kString` | `false` | `false` | `ValidateMetric` | Distance metric. |
| `encoding.type` | `FieldType::kString` | `false` | `false` | `ValidateEncodingType` | Encoding type. |
| `index.dimension` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Vector dimension. |
| `index.type` | `FieldType::kString` | `true` | `false` | `ValidateIndexType` | Index type. |

## core / quantizer

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `distance.metric` | `FieldType::kString` | `true` | `false` | `ValidateMetric` | Distance metric. |
| `encoding.type` | `FieldType::kString` | `false` | `false` | `ValidateEncodingType` | Encoding type. |
| `index.dimension` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Vector dimension. |

## core / search

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `search.parallel_number` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Search parallel number. |
| `search.thread_safe_filter` | `FieldType::kBool` | `false` | `false` | `nullptr` | Thread safe filter. |
| `search.topk` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Search-time topK override. |

## io / io

Note: `io.*` options apply to built-in file reader/writer implementations only.

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `index.path` | `FieldType::kString` | `true` | `false` | `nullptr` | Index path (built-in IO only). |
| `io.reader.mmap.lock_mode` | `FieldType::kString` | `false` | `false` | `ValidateMmapLockMode` | mmap lock mode (none/mlock/populate, built-in IO only). |
| `io.reader.type` | `FieldType::kString` | `false` | `false` | `ValidateReaderType` | Reader type (local/mmap, built-in IO only). |
| `io.verify_crc` | `FieldType::kBool` | `false` | `false` | `nullptr` | Verify section CRC on read (built-in IO only). |

## bruteforce / streamer

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `bruteforce.streamer.capacity` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Max capacity (default 1M). |

## diskann / builder

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `diskann.build.ef_construction` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | DiskANN build ef construction parameter. |
| `diskann.build.graph_node_per_sector` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | DiskANN graph node count in one sector. |
| `diskann.build.neighbor_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | DiskANN build neighbor count. |
| `diskann.build.quantized_build` | `FieldType::kBool` | `false` | `false` | `nullptr` | DiskANN build with quantized distance. |
| `diskann.build.reorder_layout` | `FieldType::kBool` | `false` | `false` | `nullptr` | DiskANN build reorder layout. |
| `diskann.build.slack_pruning_factor` | `FieldType::kDouble` | `false` | `false` | `ValidatePositiveDouble` | DiskANN build slack pruning factor. |
| `diskann.build.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | DiskANN build thread count. |
| `diskann.disk_encoding.encoding.pq.m` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ m, when DiskANN disk encoding type = PQ. |
| `diskann.disk_encoding.encoding.pq.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ max epoch, when DiskANN disk encoding type = PQ. |
| `diskann.disk_encoding.encoding.pq.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ thread count, when DiskANN disk encoding type = PQ. |
| `diskann.disk_encoding.save_origin_embedding` | `FieldType::kBool` | `false` | `false` | `nullptr` | DiskANN disk encoding save origin embedding. |
| `diskann.disk_encoding.type` | `FieldType::kString` | `false` | `false` | `ValidateEncodingType` | DiskANN disk encoding type. |

## diskann / searcher

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `diskann.search.num_nodes_to_cache` | `FieldType::kInt` | `false` | `false` | `ValidateNonNegativeInt` | DiskANN nodes to cache. |
| `diskann.search.sector_aligned_read` | `FieldType::kBool` | `false` | `false` | `nullptr` | DiskANN search sector aligned read. |

## diskann / search

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `diskann.search.beam_width` | `FieldType::kInt` | `false` | `true` | `ValidatePositiveInt` | Diskann search beam width. |
| `diskann.search.io_limit` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Diskann search IO limit. |
| `diskann.search.list_size` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | Diskann search list size. |

## extension / ckpt

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `extension.build.ckpt.count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Number of checkpoints. |
| `extension.build.ckpt.threshold` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Threshold for triggering checkpoint. |

## extension / distributebuild

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `extension.build.distribute_build.partition.centroid_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Number of centroids used for partitioning. |
| `extension.build.distribute_build.partition.dispatch_count` | `FieldType::kDouble` | `false` | `false` | `ValidatePositiveDouble` | Dispatch factor controlling how many partitions each vector is assigned to. |
| `extension.build.distribute_build.partition.kmeans.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Maximum number of KMeans iterations for partitioning. |
| `extension.build.distribute_build.partition.kmeans.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Number of threads used for KMeans clustering. |
| `extension.build.distribute_build.partition.partition_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | Number of partitions to split the dataset into. |
| `extension.build.distribute_build.partition.type` | `FieldType::kString` | `false` | `false` | `ValidateDistributeBuildPartitionType` | Partition type. |

## extension / tag

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `extension.build.tag.max_range_label_ratio` | `FieldType::kDouble` | `false` | `false` | `ValidatePositiveDouble` | Max range label ratio for label-aware construction. |
| `extension.build.tag.tag_schema` | `FieldType::kJson` | `false` | `false` | `ValidateTagSchema` | Tag schema for tag. |

## ivf / builder

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `ivf.build.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | IVF build max epoch. |
| `ivf.build.num_lists` | `FieldType::kInt` | `true` | `false` | `ValidatePositiveInt` | IVF num lists. |
| `ivf.build.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | IVF build thread count. |

## ivf / search

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `search.nprobe` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | IVF search nprobe. |

## quantizer / quantizer

| Key | Type | Required | Deprecated | Validator | Description |
| --- | ---- | -------- | ---------- | --------- | ----------- |
| `encoding.pq.m` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ m. |
| `encoding.pq.make_zero_mean` | `FieldType::kBool` | `false` | `false` | `nullptr` | PQ make zero mean. |
| `encoding.pq.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ max epoch. |
| `encoding.pq.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | PQ thread count. |
| `encoding.pq.use_opq` | `FieldType::kBool` | `false` | `false` | `nullptr` | Use OPQ. |
| `encoding.rabitq.centroid_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | RabitQ kmeans centroid count for pretrain (default 64). Larger => better fit & slower training. |
| `encoding.rabitq.max_epoch` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | RabitQ kmeans max epochs for pretrain (default 10). Higher => better fit & slower training. |
| `encoding.rabitq.quantized_bit_count` | `FieldType::kInt` | `false` | `false` | `ValidateIntInSet<1, 4, 5, 8, 9>` | RabitQ code bit width (default 4). Supported: 1, 4, 5, 8, 9. Larger => better accuracy & bigger records. |
| `encoding.rabitq.thread_count` | `FieldType::kInt` | `false` | `false` | `ValidatePositiveInt` | RabitQ kmeans thread count for pretrain (default 1). Controls training parallelism only. |