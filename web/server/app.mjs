import Fastify from "fastify";
import helmet from "@fastify/helmet";
import rateLimit from "@fastify/rate-limit";
import staticFiles from "@fastify/static";
import { fileURLToPath } from "node:url";
import { existsSync } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import { createReader } from "./overview.mjs";
import { createInvestigationReader, decodeCursor } from "./investigation.mjs";

export async function buildApp({
  reader,
  investigationReader,
  showcasePath = process.env.SHOWCASE_REPORT,
  logger = false,
} = {}) {
  const app = Fastify({ logger, bodyLimit: 1024 });
  await app.register(helmet, {
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        connectSrc: ["'self'"],
        imgSrc: ["'self'", "data:"],
        objectSrc: ["'none'"],
      },
    },
  });
  await app.register(rateLimit, { max: 120, timeWindow: "1 minute" });
  const read =
    reader ||
    createReader({
      clickhouse: process.env.CLICKHOUSE_URL || "http://127.0.0.1:28123",
      user: process.env.CLICKHOUSE_USER || "risk",
      password: process.env.CLICKHOUSE_PASSWORD || "",
      flink: process.env.FLINK_REST_URL || "http://127.0.0.1:28082",
      prometheus: process.env.PROMETHEUS_URL || "http://127.0.0.1:29090",
    });
  const investigation =
    investigationReader ||
    createInvestigationReader({
      clickhouse: process.env.CLICKHOUSE_URL || "http://127.0.0.1:28123",
      user: process.env.CLICKHOUSE_USER || "risk",
      password: process.env.CLICKHOUSE_PASSWORD || "",
    });
  for (const kind of ["customers", "timeline"]) {
    app.get(
      `/api/investigations/${kind}`,
      {
        schema: {
          querystring: {
            type: "object",
            additionalProperties: false,
            required: kind === "timeline" ? ["customer"] : [],
            properties:
              kind === "customers"
                ? {
                    q: { type: "string", maxLength: 256, default: "" },
                    decision: {
                      type: "string",
                      enum: ["ALL", "APPROVE", "REVIEW", "REJECT"],
                      default: "ALL",
                    },
                    cursor: { type: "string", maxLength: 1024, default: "" },
                  }
                : {
                    customer: { type: "string", minLength: 1, maxLength: 256 },
                    cursor: { type: "string", maxLength: 1024, default: "" },
                  },
          },
        },
      },
      async (request, reply) => {
        reply.header("Cache-Control", "no-store");
        try {
          decodeCursor(request.query.cursor, kind);
        } catch {
          return reply.code(400).send({ error: "Invalid cursor" });
        }
        try {
          return await investigation[kind](request.query);
        } catch {
          return reply.code(503).send({
            error:
              "Investigation data is unavailable or requires an integrity review. Retry after checking the source services.",
          });
        }
      },
    );
  }
  app.get("/api/showcase", async (_request, reply) => {
    reply.header("Cache-Control", "no-store");
    if (!showcasePath)
      return reply.code(404).send({ error: "Start a run with make showcase." });
    try {
      if ((await stat(showcasePath)).size > 2 * 1024 * 1024)
        throw new Error("Report too large");
      return JSON.parse(await readFile(showcasePath, "utf8"));
    } catch {
      return reply
        .code(503)
        .send({ error: "Showcase report is not available yet." });
    }
  });
  const cache = new Map();
  app.get("/api/health", async () => ({ status: "ok" }));
  app.get(
    "/api/overview",
    {
      schema: {
        querystring: {
          type: "object",
          additionalProperties: false,
          properties: {
            window: {
              type: "string",
              enum: ["15m", "1h", "24h"],
              default: "1h",
            },
          },
        },
      },
    },
    async (request, reply) => {
      reply.header("Cache-Control", "no-store");
      const window = request.query.window;
      const existing = cache.get(window);
      if (existing && existing.expires > Date.now()) return existing.value;
      const pending = existing?.pending || read(window);
      cache.set(window, { ...existing, pending });
      try {
        const value = await pending;
        cache.set(window, { value, expires: Date.now() + 5000 });
        return reply.header("Cache-Control", "no-store").send(value);
      } catch {
        cache.delete(window);
        return reply.code(503).send({
          error:
            "Live data is unavailable. Check the local services or switch to demo data.",
        });
      }
    },
  );
  const root = fileURLToPath(new URL("../dist", import.meta.url));
  if (existsSync(root)) {
    await app.register(staticFiles, { root, cacheControl: true, maxAge: "1h" });
    app.setNotFoundHandler((request, reply) =>
      request.url.startsWith("/api/")
        ? reply.code(404).send({ error: "Not found" })
        : reply.sendFile("index.html", { maxAge: 0 }),
    );
  }
  return app;
}
