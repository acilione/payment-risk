import { useMemo } from "react";
import { demoOverview } from "../../demo";
import { useJsonResource } from "../../hooks/useJsonResource";
import type { Overview, WindowKey } from "../../types";
export function useOverview(
  demo: boolean,
  window: WindowKey,
  enabled: boolean,
  refresh: number,
) {
  const live = useJsonResource<Overview>(
    !demo && enabled ? `overview?window=${window}` : null,
    refresh,
    15000,
  );
  const saved = useMemo(
    () => (demo ? demoOverview(window) : null),
    [demo, window],
  );
  return {
    data: demo ? saved : live.data,
    loading: !demo && live.loading,
    error: live.error
      ? "Live data is unavailable. Start the local services or switch to demo data."
      : "",
  };
}
