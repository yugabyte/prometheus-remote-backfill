# Tools to backfill Prometheus remote storage

This repo is a fork of the [prometheus-remote-backfill](https://github.com/knyar/prometheus-remote-backfill) project
that adds Yugabyte-specific enhancements to make it easier to export and collect metrics from YugabyteDB Anywhere on a
per-Universe basis.

The promdump tool remains backward compatible but adds additional modes and flags to streamline collection of Yugabyte
metrics data.

* `promdump` - dumps time series data from Prometheus server into JSON files.

This tools can be used to backfill metric data from Prometheus into other metrics stores such as VictoriaMetrics. JSON
files are used as intermediary storage format to allow manual changes to metric metadata (metric name and labels).

The `promremotewrite` utility that was included in the original `prometheus-remote-backfill` has been removed due to
outdated dependencies with known security issues.

## promdump

By default `promdump` issues a separate query for each 24 hours worth of data, and writes each resulting batch of
samples into a separate file. First is necessary to avoid overloading Prometheus server with queries with very large
response size, second is to prevent `promremotewrite` from using too much RAM (each JSON file needs to be loaded in
memory).

Yugabyte metrics have very high cardinality (lots of label values, resulting in many time series per metric name), so
this version of the utility has a back-off mechanism, where if the Prometheus server returns a "too many samples"
error, the batch size will be halved and the operation retried. 

## promremotewrite

This utility is not used at Yugabyte. It has been removed from the repo due to known vulnerabilities in its
dependencies.

## Example

Dump all data of `node_filesystem_free` metric for the last year, issuing a separate query for each 12hrs of data,
storing 24hrs worth of data in each file:

    promdump --url=http://localhost:9090 \
      --metric='node_filesystem_free{job="node"}' \
      --out=fs_free.json --batch=12h --batches_per_file=2 --period=8760h

## Command Line Reference

Note that this section is a reference guide for the complete command line interface of the utility. The vast majority
of these flags override a specific behaviour and should only be used situationally. The set of required flags is very
small.

Flags may be specified using a single preceding dash (`-flagname`) or two preceding dashes (`--flagname`).

Flag names containing multiple words use underscore (`_`) as a word separator (`--multi_word_flag`).

Most flags may be specified with the equals sign (`=`) or space (` `) between the flag name and value. However, this
may lead to ambiguity that causes command failure or inconsistent behaviour, so it is recommended to always use the
equals sign as the separator (`--flagname=value`).

Flags that accept multiple values use commas (`,`) as a separator (`--multi_value_flag=foo,bar,baz`).

### Instant Flags

Flags in this section are of the "print something and exit" variety.

| Canonical Flag Name | Alias(es) | Added In        | Default | Required? | Description                                                                                   |
|---------------------|-----------|-----------------|---------|-----------|-----------------------------------------------------------------------------------------------|
| `--help`            | `-h`      | Initial Release |         | Optional  | Prints the list of available command line flags and brief descriptions, then exits            |
| `--version`         | `-v`      | v0.1.2          |         | Optional  | Prints the `promdump` version number, commit hash, and build date, then exits.                |
| `--list_universes`  |           | v0.5.1          |         | Optional  | Prints the list of Universes known to YBA, then exits.<br/><br/>Requires a `--yba_api_token`. | 

### Logging

Flags in this section control the logging behaviour of the utility.

| Canonical Flag Name | Alias(es) | Added In | Default        | Required? | Description                                                                                   |
|---------------------|-----------|----------|----------------|-----------|-----------------------------------------------------------------------------------------------|
| `--debug`           |           | v0.1.3   | `false`        | Optional  | Enables debug logging.                                                                        |
| `--log_to_console`  |           | v0.5.2   | `true`         | Optional  | Write log messages to the console.                                                            |
| `--log_to_file`     |           | v0.5.2   | `true`         | Optional  | Write log messages to a log file.                                                             |
| `--log_filename`    |           | v0.5.2   | `promdump.log` | Optional  | Specifies the name of the file to which to write log messages when `--log_to_file` is `true`. |

### Prometheus Flags

Flags in this section control the connection to Prometheus.

| Canonical Flag Name              | Alias(es) | Added In        | Default                 | Required?  | Description                                                                                                                                                                                                                     |
|----------------------------------|-----------|-----------------|-------------------------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--url`                          |           | Initial Release | `http://localhost:9090` | Optional   | The URL to use when querying the YBA Prometheus server.                                                                                                                                                                         |
| `--skip_prom_host_verification`  |           | v0.5.1          | `false`                 | Optional   | Disables certificate verification for TLS connections to the YBA Prometheus.<br/><br/>Note that the YBA Prometheus ships with TLS disabled.<br/><br/>This flag will be ignored if the `--url` flag uses `http` as the protocol. |
| `--prometheus_api_timeout`       |           | v0.5.1          | 10                      | Optional   | Requests to the Prometheus API will time out at the HTTP layer after this many seconds.                                                                                                                                         |
| `--prometheus_retry_count`       |           | v0.6.0          | 10                      | Optional   | How many times to retry Prometheus API requests before aborting the dump.                                                                                                                                                       |
| `--prometheus_retry_backoff`     |           | v0.6.0          | true                    | Optional   | Whether to apply a backoff algorithm to Prometheus API requests. If set to true, extends the retry delay between retries.                                                                                                       |
| `--prometheus_retry_delay`       |           | v0.6.0          | 1                       | Optional   | The initial retry delay for Prometheus requests in seconds. The actual delay depends on whether backoff is enabled.                                                                                                             |
| `--prometheus_retry_max_backoff` |           | v0.6.0          | 15                      | Optional   | The maximum retry delay for Prometheus requests in seconds. Only used if backoff is enabled.                                                                                                                                    |

### YBA API Mode

In YBA API mode, `promdump` will connect to the specified YBA node and use the provided `--universe_name` or
`--universe_uuid` value to look up the `node_prefix` needed to export the Yugabyte metrics for the corresponding
Universe.

| Canonical Flag Name            | Alias(es)        | Added In | Default     | Required? | Description                                                                                                                                                                                                                                                                                                                               |
|--------------------------------|------------------|----------|-------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--yba_api_token`              |                  | v0.5.1   |             | Required  | A [YBA API authentication token](https://api-docs.yugabyte.com/docs/yugabyte-platform/f10502c9c9623-yugabyte-db-anywhere-api-overview#get-an-api-token) for a user account with the privileges required to list customers and list Universes.                                                                                             |
| `--universe_name`              |                  | v0.5.1   |             | Required* | The name of the Universe for which to export metrics.<br/><br/>*One of `--universe_name` or `--universe_uuid` is required in YBA API mode.                                                                                                                                                                                                |
| `--universe_uuid`              |                  | v0.5.1   |             | Required* | The UUID of the Universe for which to export metrics.<br/><br/>*One of `--universe_name` or `--universe_uuid` is required in YBA API mode.                                                                                                                                                                                                |
| `‑‑yba_api_hostname`           | `‑‑yba_hostname` | v0.5.1   | `localhost` | Optional  | The hostname or IP address to use when making YBA API calls.                                                                                                                                                                                                                                                                              |
| `--skip_yba_host_verification` |                  | v0.5.1   | `false`     | Optional  | Disables certificate verification for TLS connections to the YBA API.<br/><br/>**WARNING:** This flag instructs promdump to accept the YBA node's TLS certificate regardless of validity, which may be a potential security risk. It is strongly recommended to use certificates issued by a certificate authority on production systems. |                                                                                                                                                                                                                                        
| `--yba_api_use_tls`            |                  | v0.5.1   | `true`      | Optional  | Set to `false` to disable TLS for connections to the YBA API.<br/><br/>With TLS disabled, the utility will attempt to make HTTP requests to the YBA API using an unencrypted connection on port 80. Disabling TLS is almost always an error.                                                                                              | 
| `--yba_api_timeout`            |                  | v0.5.1   | 10          | Optional  | Requests to the YBA API will time out after this many seconds.                                                                                                                                                                                                                                                                            |

### Legacy Mode

In Legacy mode, users must provide the `node_prefix` value for the Universe for which they wish to export the Yugabyte
metrics.

| Canonical Flag Name        | Alias(es) | Added In | Default | Required? | Description                                                                                                                                                                                                                                                                   |
|----------------------------|-----------|----------|---------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--node_prefix`            |           | v0.1.2   |         | Required  | The `node_prefix` value for the Yugabyte Universe for which metrics should be exported. For example, `yb-prod-appname`. Simple validations will be performed on the specified value, such as verifying that it starts with `yb-` and that it does not end with a node number. |
| `--node_prefix_validation` |           | v0.3.0   | `true`  | Optional  | Set to `false` to disable validation of the node prefix value. This may be required if the customer is using a non-standard environment name or in case of unforeseen issues with the validation.                                                                             |

### Manual Mode

In manual mode, the specified PromQL expression will be sent to Prometheus and the resulting metrics written to a series of numbered files starting with the specified file prefix.

Manual mode may be combined with either YBA API mode or Legacy Mode to export the Yugabyte metrics along with a custom metric specification.

| Canonical Flag Name | Alias(es) | Added In        | Default | Required? | Description                                                             |
|---------------------|-----------|-----------------|---------|-----------|-------------------------------------------------------------------------|
| `--metric`          |           | Initial Release |         | Required  | Specifies a PromQL expression to send to Prometheus for export.         |
| `--out`             |           | Initial Release |         | Required  | The file prefix to use for writing the specified metric(s) out to disk. |

### Query Flags

Flags in this section control what metrics data the utility requests from Prometheus for export.

#### Time Window

The three flags that control the time window for which to export the Prometheus data are `--start_time`, `--end_time`,
and `--period`. Up to two of these flags may be specified.

If all three time window flags are omitted, the utility will export the most recent 7 days of data.

If only `--period` is specified, the utility will export the most recent metrics for the specified period. For example, `--period=20m` will export the most recent 20 minutes of metrics. 

If `--period` and one of `--start_time` or `--end_time` are specified, the utility will export metrics for a window with the specified duration starting at or ending at the specified timestamp. 

If `--start_time` and `--end_time` are specified, the utility will export metrics that fall between the specified timestamps.

Specifying all three time window flags is an error.

| Canonical Flag Name | Alias(es)     | Added In        | Default | Required? | Description                                                                                                                                                                                                                      |
|---------------------|---------------|-----------------|---------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--start_time`      |               | Initial Release |         | Optional  | The timestamp of the start of the window for which to export data. Timestamps must be specified in RFC3339 format, e.g. `2023-03-13T01:00:00-0100`. It is recommended to specify timestamps in UTC, e.g. `2023-03-13T00:00:00Z`. |
| `--end_time`        | `--timestamp` | Initial Release | Now     | Optional  | The timestamp of the end of the window for which to export data. Timestamps must be specified in RFC3339 format, e.g. `2023-03-13T01:00:00-0100`. It is recommended to specify timestamps in UTC, e.g. `2023-03-13T00:00:00Z`.   |
| `--period`          |               | Initial Release | 7d      | Optional  | The length of the time window for which to export data. Units are specified using standard abbreviations such as `d` for days, `h` for hours, or `m` for minutes.                                                                |

#### Batching

Flags in this section control how metrics are batched up when requesting data from Prometheus and writing data to
export files. Prometheus limits the number of samples that may be loaded into memory by any particular request, so the
batch size must be small enough not to exceed this limit.

This fork of the `promdump` utility performs automatic batch size backoff if Prometheus returns an error indicating
that too many samples would be loaded into memory. The backoff mechanism halves the batch size and retries.

| Canonical Flag Name  | Alias(es) | Added In        | Default | Required? | Description                                                                                              |
|----------------------|-----------|-----------------|---------|-----------|----------------------------------------------------------------------------------------------------------|
| `--batch`            |           | Initial Release | 24h     | Optional  | The batch duration to use when requesting metrics from Prometheus for export.                            |
| `--batches_per_file` |           | Initial Release | 1       | Optional  | The number of batches to write into each output file. There is rarely a good reason to change this flag. |

#### Metric Filtering

Flags in this section are applicable to YBA API Mode and Legacy Mode (see below). They do not apply to manual mode.

These flags will filter the metrics when exporting data from Prometheus. Most of these flags enable or disable specific
exports or jobs. In general, these settings should be left at their defaults unless there is a compelling reason to do
otherwise.  

##### Exporters

These metrics are labelled with the `export_type` label in Prometheus.

| Canonical Flag Name | Output Filename        | Added In | Default | Required? | Description                                                                                                                                                            |
|---------------------|------------------------|----------|---------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--master`          | `master_export.#####`  | v0.1.2   | `true`  | Optional  | Enables or disables collection of the metrics for the Yugabyte `master` process.                                                                                       |
| `--node`            | `node_export.#####`    | v0.1.2   | `true`  | Optional  | Enables or disables collection of the `node_exporter` OS level metrics.                                                                                                |
| `--tserver`         | `tserver_export.#####` | v0.1.2   | `true`  | Optional  | Enables or disables collection of the metrics for the Yugabyte tablet server. These metrics are scraped from the `tserver` process and mostly cover the storage layer. |
| `--ycql`            | `cql_export.#####`     | v0.1.2   | `true`  | Optional  | Enables or disables collection of the metrics for the Cassandra-compatible YCQL query layer. These metrics are scraped from the `tserver` process.                     |
| `--ysql`            | `ysql_export.#####`    | v0.1.2   | `true`  | Optional  | Enables or disables collection of the metrics for the Postgres-compatible YSQL query layer. These metrics are scraped from the `tserver` process.                      |

##### Jobs

These metrics are labelled with the `job` label in Prometheus.

| Canonical Flag Name | Output Filename    | Added In | Default | Required? | Description                                                                                |
|---------------------|--------------------|----------|---------|-----------|--------------------------------------------------------------------------------------------|
| `--platform`        | `platform.#####`   | v0.1.4   | `true`  | Optional  | Enables or disables collection of the `platform` metrics produced by YBA.                  |
| `--prometheus`      | `prometheus.#####` | v0.1.4   | `false` | Optional  | Enables or disables collection of the `prometheus` metrics produced by the YBA Prometheus. |

#### Node Filtering

If metrics are required for specific database nodes (for example, because only a subset of nodes has experienced a
problem or because there is a large number of nodes in the Universe), the following flags can be used to export metrics
for only the specified nodes.

| Canonical Flag Name | Alias(es) | Added In | Default | Required? | Description                                                                                                                                                                                                                                                                                 |
|---------------------|-----------|----------|---------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--nodes`           |           | v0.2.0   |         | Optional  | Collect metrics for only the specified subset of nodes. Accepts a comma separated list of node numbers or ranges. For example, `--nodes=1,3-6,14` would collect metrics for nodes 1, 3, 4, 5, 6, and 14. Mutually exclusive with `--instances`.                                             |
| `--instances`       |           | v0.2.0   |         | Optional  | Collect metrics for only the specified subset of nodes. Accepts a comma separated list of instance names. For example, `--instances=yb-prod-appname-n1,yb-prod-appname-n3,yb-prod-appname-n4,yb-prod-appname-n5,yb-prod-appname-n6,yb-prod-appname-n14`. Mutually exclusive with `--nodes`. |

#### Collection Level

It can be extremely challenging to export tserver metrics from systems with very large numbers of nodes, tables, or
tablets due to the sheer volume of data. The following flag can be used to apply the YBA "minimal" collection level
rules to the dump, reducing the amount of data dumped and reducing dump runtime and size. Note that this setting can
only *reduce* the amount of metrics data collected; if the YBA collection level is set to any level lower than
`MINIMAL`, (e.g. if metrics collection is `OFF` in YBA), no metrics will be dumped. You can't dump what was never
collected.

| Canonical Flag Name  | Alias(es) | Added In | Default  | Required? | Description                                                                                                                                 |
|----------------------|-----------|----------|----------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `--collection_level` |           | v0.7.0   | `normal` | Optional  | Limit the size of the dump by collecting only the metrics associated with the YBA "minimal" collection level. One of `normal` or `minimal`. |

Minimal collection mode limits promdump to collecting only the following tserver metrics (where `*` matches anything):

`async_replication_committed_lag_micros`,
`async_replication_sent_lag_micros`,
`block_cache_*`,
`cpu_stime`,
`cpu_utime`,
`follower_lag_ms`,
`follower_memory_pressure_rejections`,
`generic_current_allocated_bytes`,
`generic_heap_size`,
`glog*`,
`handler_latency_outbound_call_queue_time*`,
`handler_latency_outbound_transfer*`,
`handler_latency_yb_client*`,
`handler_latency_yb_consensus_ConsensusService*`,
`handler_latency_yb_cqlserver_CQLServerService*`,
`handler_latency_yb_cqlserver_SQLProcessor*`,
`handler_latency_yb_master*`,
`handler_latency_yb_redisserver_RedisServerService_*`,
`handler_latency_yb_tserver_TabletServerService*`,
`handler_latency_yb_ysqlserver_SQLProcessor*`,
`hybrid_clock_skew`,
`involuntary_context_switches*`,
`leader_memory_pressure_rejections`,
`log_wal_size`,
`majority_sst_files_rejections`,
`operation_memory_pressure_rejections`,
`rocksdb_current_version_sst_files_size`,
`rpc_connections_alive`,
`rpc_inbound_calls_created`,
`rpc_incoming_queue_time*`,
`rpcs_in_queue*`,
`rpcs_queue_overflow`,
`rpcs_timed_out_in_queue`,
`spinlock_contention_time*`,
`threads_running*`,
`threads_started*`,
`transaction_pool_cache*`,
`voluntary_context_switches*`,
`yb_ysqlserver_active_connection_total`,
`yb_ysqlserver_connection_over_limit_total`,
`yb_ysqlserver_connection_total`,
`yb_ysqlserver_new_connection_total`

This list (and the corresponding `promdump` code) was derived from the metrics level params configuration file
`minimal_level_params.json` from the 2024.2.0-b145 YBA release.

### Output Flags

Flags in this section control aspects of how the exported data are written to disk.

| Canonical Flag Name           | Alias(es) | Added In | Default   | Required? | Description                                                                                                                                                                                                                                             |
|-------------------------------|-----------|----------|-----------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--tar`                       |           | v0.5.1   | `true`    | Optional  | Controls whether the exported metrics files are packaged into a tar bundle at the end of the run.                                                                                                                                                       |
| `--tar_compression_algorithm` |           | v0.5.1   | `gzip`    | Optional  | The compression algorithm to use when creating a tar bundle. One of `gzip`, `bzip2`, or `none`.<br/><br/>Only applicable if `--tar` is enabled.                                                                                                         |
| `--tar_filename`              |           | v0.5.1   | Generated | Optional  | Specifies the filename to use for the tar bundle.<br/><br/>If not specified, a default filename of the form `promdump-yb-prod-appname-20240704-143423` will be generated based on the node_prefix of the Universe and the date and time that `promdump` was run. |  
| `--keep_files`                |           | v0.5.1   | `false`   | Optional  | By default, the export files generated by `promdump` will be deleted after the tar bundle has been generated.<br/><br/>Set this flag to `true` to preserve these files.                                                                                          |

## Compile

```
docker run --rm \
  -v "$PWD/promdump":/promdump \
  -w /promdump golang:1.19 \
  go build \
  -ldflags=" -X 'main.CommitHash=$(git rev-parse HEAD)' -X 'main.BuildTime=$(date -Iseconds)'"
```

## License

Licensed under MIT license.
