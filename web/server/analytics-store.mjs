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
    const settings = ` SETTINGS readonly=${writable ? 0 : 1}, max_execution_time=${writable ? 60 : 4}, max_memory_usage=${writable ? 536870912 : 268435456}, max_bytes_before_external_group_by=67108864, max_bytes_before_external_sort=67108864`;
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
