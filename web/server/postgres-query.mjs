// Local verification helper. Input stays on stdin; connection secrets stay in the environment.
import { postgresPool, readTransaction } from "./postgres-store.mjs";
let input = "";
for await (const chunk of process.stdin) {
  input += chunk;
  if (input.length > 1048576) throw new Error("Query request too large");
}
const { query, parameters = [] } = JSON.parse(input);
const pool = postgresPool();
try {
  const rows = await readTransaction(
    pool,
    async (db) => (await db.query(query, parameters)).rows,
  );
  process.stdout.write(JSON.stringify(rows));
} catch (error) {
  const transient = [
    "ECONNREFUSED",
    "ECONNRESET",
    "ENOTFOUND",
    "EAI_AGAIN",
    "ETIMEDOUT",
    "57P01",
    "57P02",
    "57P03",
    "53300",
  ].includes(error.code);
  process.stderr.write(
    "PostgreSQL verification failed: " + (error.code || "unknown") + "\n",
  );
  process.exitCode = transient ? 75 : 2;
} finally {
  await pool.end();
}
