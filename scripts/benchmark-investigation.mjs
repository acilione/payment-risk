// Isolated synthetic database. No inserts, resets or deletions in risk.*.
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { cpus, totalmem, platform } from "node:os";
import { execFileSync } from "node:child_process";
import { createInvestigationReader } from "../web/server/investigation.mjs";
import { createReader } from "../web/server/overview.mjs";
const config = { clickhouse: process.env.CLICKHOUSE_URL || "http://127.0.0.1:28123", user: process.env.CLICKHOUSE_USER || "risk", password: process.env.CLICKHOUSE_PASSWORD || "", flink: "http://127.0.0.1:28082", prometheus: "http://127.0.0.1:29090" };
const database = "risk_benchmark_" + randomUUID().replaceAll("-", "");
const samples = JSON.parse(await readFile(new URL("../web/src/investigation-fixture.json", import.meta.url))).cases.flatMap(s => s.timeline.map(t => t.decision));
async function sql(query) {
  const response = await fetch(config.clickhouse, { method: "POST", headers: { "X-ClickHouse-User": config.user, "X-ClickHouse-Key": config.password }, body: query, signal: AbortSignal.timeout(60000) });
  if (!response.ok) throw new Error(`ClickHouse status ${response.status}: ${(await response.text()).slice(0,200)}`);
  return response.text();
}
const fetcher = async (url, options = {}) => {
  if (String(url).startsWith(config.clickhouse)) return fetch(url, { ...options, body: options.body.replaceAll("risk.", database + ".") });
  return { ok: true, json: async () => ({ jobs: [], status: "success", data: { result: [] } }) };
};
const reader = createInvestigationReader(config, fetcher);
const overview = createReader(config, fetcher);
const report = { sourceCommit: execFileSync("git", ["rev-parse", "HEAD"], {encoding:"utf8"}).trim(), generatedAt: new Date().toISOString(), environment:{platform:platform(),cpus:cpus().length,memoryBytes:totalmem()}, scope: "Exact production SQL in an isolated synthetic database; health probes stubbed; excludes Kafka and HTTP server routing", budgets:{requestP95Ms:2000,errors:0}, stages:[] };
const percentile = (a,p) => [...a].sort((x,y)=>x-y)[Math.ceil(a.length*p)-1];
const eventBase = Date.now() - 60000;
let created = false;
try {
  await sql(`CREATE DATABASE ${database}`); created = true;
  const schema = await readFile(new URL("../infrastructure/docker/clickhouse.sql",import.meta.url),"utf8");
  for (const statement of schema.replace(/--[^\n]*/g, "").split(";").map(s=>s.trim()).filter(Boolean)) {
    if (statement.startsWith("CREATE DATABASE")) continue;
    await sql(statement.replaceAll("risk.",database+"."));
  }
  let inserted=0;
  for (const count of [1000,10000,50000]) {
    while (inserted<count) {
      const rows=[];
      for (let i=inserted;i<Math.min(inserted+500,count);i++) {
        const source=samples[i%samples.length];
        const row={...source,decision_id:`risk_load_${i}`,evaluation_id:`eval_load_${i}`,event_id:`event_load_${i}`,transaction_id:`txn_load_${i}`,customer_id:`customer_${String(i%1000).padStart(4,"0")}`,event_time:eventBase+Math.floor(i/2000)*1000,processed_at:Date.now(),source_topic:"risk.decisions",source_partition:i%3,source_offset:i,rule_evidence:JSON.stringify(source.rule_evidence)};
        rows.push(row);
        if(i%10===0)rows.push({...row,source_offset:count+i});
      }
      await sql(`INSERT INTO ${database}.evaluations FORMAT JSONEachRow\n`+rows.map(r=>JSON.stringify(r)).join("\n"));
      inserted=Math.min(inserted+500,count);
    }
    const stage={logicalPayments:count,physicalDeliveries:Math.ceil(count*1.1),checks:[],measurements:[]};
    report.stages.push(stage);
    for (const [operation, run] of [
      ["customers",()=>reader.customers({q:"customer_"})],
      ["timeline",()=>reader.timeline({customer:"customer_0000"})],
      ["overview",()=>overview("24h")],
    ]) {
      for (const concurrency of [1,4]) {
        const durations=[]; let errors=0;
        for(let round=0;round<3;round++) await Promise.all(Array.from({length:concurrency},async()=>{
          const start=performance.now();
          try {const result=await run();
            if(operation==="overview" && result.totals.transactions!==count)throw new Error("Duplicate counting");
            if(operation==="customers" && (result.items.length!==20 || result.items.some(c=>c.payments!==count/1000)))throw new Error("Customer counts differ");
            if(operation==="timeline" && result.customer.payments!==count/1000)throw new Error("Timeline count differs");
          } catch(error) {if(/Duplicate counting|counts differ|count differs/.test(error.message))throw error;errors++;}
          durations.push(performance.now()-start);
        }));
        stage.measurements.push({operation,concurrency,samples:durations.length,p50Ms:percentile(durations,.5),p95Ms:percentile(durations,.95),maxMs:Math.max(...durations),errors,withinBudget:errors===0&&percentile(durations,.95)<=report.budgets.requestP95Ms});
      }
    }
    // Assert pagination coverage at a manageable scale; never use OFFSET or merge deliveries in JS.
    if(count===10000) {
      const seen=new Set();let cursor="";
      do {const page=await reader.customers({q:"customer_",cursor});for(const c of page.items){if(seen.has(c.customer_id))throw new Error("Repeated page customer");seen.add(c.customer_id);}cursor=page.nextCursor;}while(cursor);
      if(seen.size!==1000)throw new Error("Customer pagination gap");
      stage.checks.push("1000 customers across 50 pages, no duplicate or missing IDs");
    }
    await mkdir("artifacts",{recursive:true});await writeFile("artifacts/investigation-load.json",JSON.stringify(report,null,2)+"\n");
  }
  // All pages include tied timestamps in the synthetic customer. Check duplicate-free IDs.
  const ids=new Set();let cursor="";
  do {const page=await reader.timeline({customer:"customer_0000",cursor});for(const d of page.items){if(ids.has(d.evaluation_id))throw new Error("Repeated timeline ID");ids.add(d.evaluation_id);}cursor=page.nextCursor;}while(cursor);
  if(ids.size!==50)throw new Error("Timeline pagination gap");
  report.stages.at(-1).checks.push("50 customer payments across 3 timeline pages; physical retries counted once");
} finally {
  await mkdir("artifacts",{recursive:true}); await writeFile("artifacts/investigation-load.json",JSON.stringify(report,null,2)+"\n");
  if(created) await sql(`DROP DATABASE ${database} SYNC`);
}
console.log(JSON.stringify(report,null,2));
