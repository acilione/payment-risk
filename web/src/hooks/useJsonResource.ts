import { useEffect, useState } from "react";
import { readJson } from "../lib/api";
interface ResourceState<T> {
  path: string | null;
  data: T | null;
  error: Error | null;
  loading: boolean;
}
/** A null path disables the request. Polling waits for the previous read to finish. */
export function useJsonResource<T>(
  path: string | null,
  refresh = 0,
  pollMs?: number,
) {
  const [state, setState] = useState<ResourceState<T>>({
    path: null,
    data: null,
    error: null,
    loading: false,
  });
  useEffect(() => {
    if (path === null) return;
    const requestPath = path;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    async function read() {
      setState((previous) => ({
        path,
        data: previous.path === path ? previous.data : null,
        error: null,
        loading: true,
      }));
      try {
        const data = await readJson<T>(requestPath, controller.signal);
        if (!controller.signal.aborted)
          setState({ path, data, error: null, loading: false });
      } catch (error) {
        if (!controller.signal.aborted) {
          setState((previous) => ({
            ...previous,
            error:
              error instanceof Error ? error : new Error("Data unavailable"),
            loading: false,
          }));
        }
      } finally {
        if (!controller.signal.aborted && pollMs !== undefined)
          timer = setTimeout(read, pollMs);
      }
    }
    void read();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [path, refresh, pollMs]);
  // Never render the previous customer's or time window's response under a new query.
  if (path === null || state.path !== path)
    return { data: null, error: null, loading: path !== null };
  return state;
}
