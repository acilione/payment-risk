import { publishAnalytics } from "./analytics-publisher.mjs";
const config = {
  clickhouse: process.env.CLICKHOUSE_URL || "http://127.0.0.1:28123",
  user: process.env.CLICKHOUSE_USER || "risk",
  password: process.env.CLICKHOUSE_PASSWORD || "",
};
let stopping = false;
process.on("SIGTERM", () => {
  stopping = true;
});
process.on("SIGINT", () => {
  stopping = true;
});
while (!stopping) {
  try {
    console.log(
      JSON.stringify({
        event: "analytics_published",
        ...(await publishAnalytics(config)),
      }),
    );
  } catch (error) {
    console.error(
      JSON.stringify({
        event: "analytics_publication_failed",
        error: error.message,
      }),
    );
    if (process.argv.includes("--once")) process.exitCode = 1;
  }
  if (process.argv.includes("--once")) break;
  for (let i = 0; i < 60 && !stopping; i++)
    await new Promise((resolve) => setTimeout(resolve, 1000));
}
