import pg from "pg";

export function postgresPool(options = {}) {
  const pool = new pg.Pool({
    host: process.env.PGHOST || "127.0.0.1",
    port: Number(process.env.PGPORT || 25432),
    database: process.env.PGDATABASE || "risk",
    user: process.env.PGUSER || "risk",
    password: process.env.PGPASSWORD || process.env.RISK_DB_PASSWORD,
    max: 8,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
    statement_timeout: 5000,
    application_name: "payment-risk-api",
    ...options,
  });
  // The pool removes broken idle connections. Active requests surface their own errors as 503.
  pool.on("error", () => {});
  return pool;
}

export async function readTransaction(pool, action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
