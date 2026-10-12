// Database access shared by the publication worker and read-only serving reader.
export function analyticsStore(
  config,
  fetcher = fetch,
  { writable = false } = {},
) {
  return async function sql(query, parameters = {}, rows = false) {
    const url = new URL(config.clickhouse);
    for (const [key, value] of Object.entries(parameters))
      url.searchParams.set(`param_${key}`, String(value));
    url.searchParams.set("wait_end_of_query", "1");
    const mib = 1024 * 1024;
    const limits = {
      readonly: writable ? 0 : 1,
      max_execution_time: writable ? 60 : 4,
      max_threads: writable ? 2 : 4,
      max_block_size: writable ? 2048 : 8192,
      max_memory_usage: (writable ? 512 : 256) * mib,
      max_bytes_before_external_group_by: (writable ? 16 : 64) * mib,
      max_bytes_before_external_sort: 64 * mib,
      ...(writable
        ? {
            // Partition intermediate groups before finalization, including workloads
            // below ClickHouse's default 100,000-key conversion threshold.
            group_by_two_level_threshold: 10000,
            group_by_two_level_threshold_bytes: 16 * mib,
            aggregation_memory_efficient_merge_threads: 1,
            // INSERT SELECT buffering is separate from aggregation spill memory.
            max_insert_threads: 1,
            min_insert_block_size_rows: 2048,
            min_insert_block_size_bytes: 4 * mib,
          }
        : {}),
    };
    const settings =
      " SETTINGS " +
      Object.entries(limits)
        .map(([name, value]) => `${name}=${value}`)
        .join(", ");
    const response = await fetcher(url.toString(), {
      method: "POST",
      signal: AbortSignal.timeout(writable ? 65000 : 5000),
      headers: {
        "X-ClickHouse-User": config.user,
        "X-ClickHouse-Key": config.password,
      },
      body: query + settings + (rows ? " FORMAT JSON" : ""),
    });
    if (!response.ok)
      throw new Error(`Analytics query failed (${response.status})`);
    return rows ? (await response.json()).data : response.text();
  };
}
