import postgres from "postgres";
import { OTEL_ENABLED, withSpan } from "./telemetry.ts";

const DATABASE_URL =
  Deno.env.get("DATABASE_URL") ??
  "postgres://kanban:kanban@127.0.0.1:5432/kanban_db";

const _sql = postgres(DATABASE_URL, {
  max: 10,
  idle_timeout: 20,
  connect_timeout: 10,
});

// deno-lint-ignore no-explicit-any
const sql: typeof _sql = OTEL_ENABLED
  ? new Proxy(_sql, {
      apply(_target, _thisArg, args) {
        const [strings] = args as [TemplateStringsArray, ...unknown[]];
        const statement = Array.isArray(strings)
          ? strings.join("$?")
          : "unknown";
        return withSpan(
          "db.query",
          { "db.statement": statement, "db.system": "postgresql" },
          () => Reflect.apply(_target, _thisArg, args),
        );
      },
    })
  : _sql;

export default sql;
