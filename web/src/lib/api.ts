/** All dashboard reads use the same timeout, JSON checks and cancellation signal. */
export async function readJson<T>(
  path: string,
  signal: AbortSignal,
): Promise<T> {
  const response = await fetch(`${import.meta.env.BASE_URL}api/${path}`, {
    signal: AbortSignal.any([signal, AbortSignal.timeout(12000)]),
    cache: "no-store",
  });
  if (
    !response.ok ||
    !response.headers.get("content-type")?.includes("application/json")
  ) {
    throw new Error("The requested data is unavailable.");
  }
  return response.json() as Promise<T>;
}
