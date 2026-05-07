// validate.ts — type-check `keto-namespaces.config.ts` against the Keto OPL
// type definitions and render the relation/permission graph in plain English.
//
// Run via `npm run validate`. Exits 0 on success, non-zero on any TS error
// (matching the same compile gate Keto's directory-watcher applies before
// reloading namespaces in production).
//
// This file is operator/CI tooling only — never imported from production
// paths. `feedback_no_shelling_out.md` does not apply (no subprocess from
// runtime code), but we still avoid `child_process` here in favour of the
// programmatic TypeScript compiler API.

import * as path from "node:path"
import * as fs from "node:fs"
import * as ts from "typescript"

const HERE = path.dirname(new URL(import.meta.url).pathname)
const CONFIG_FILE = path.join(HERE, "keto-namespaces.config.ts")
const TYPES_DIR = path.resolve(
  HERE,
  "../../../forks/keto/contrib/namespace-type-lib",
)

function fail(msg: string): never {
  console.error(`validate.ts: ${msg}`)
  process.exit(1)
}

if (!fs.existsSync(CONFIG_FILE)) {
  fail(`config file not found: ${CONFIG_FILE}`)
}
if (!fs.existsSync(path.join(TYPES_DIR, "index.d.ts"))) {
  fail(
    `@ory/keto-namespace-types not found at ${TYPES_DIR}; ` +
      `expected sibling clone at forks/keto/contrib/namespace-type-lib/`,
  )
}

// ─── 1. tsc compile (matches `tsc --noLib --noEmit --types <file>`) ──────────

// Inject a path mapping so `import "@ory/keto-namespace-types"` resolves to
// the local fork's `index.d.ts` without needing a node_modules link.
const compilerOptions: ts.CompilerOptions = {
  noLib: true,
  noEmit: true,
  strict: false,
  target: ts.ScriptTarget.ES2020,
  module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Bundler,
  types: [],
  paths: {
    "@ory/keto-namespace-types": [path.join(TYPES_DIR, "index.d.ts")],
  },
  baseUrl: HERE,
}

const program = ts.createProgram([CONFIG_FILE], compilerOptions)
const diagnostics = ts.getPreEmitDiagnostics(program)
if (diagnostics.length > 0) {
  const formatHost: ts.FormatDiagnosticsHost = {
    getCanonicalFileName: (f) => f,
    getCurrentDirectory: () => HERE,
    getNewLine: () => "\n",
  }
  console.error(ts.formatDiagnosticsWithColorAndContext(diagnostics, formatHost))
  fail(`TypeScript compile failed (${diagnostics.length} diagnostic(s))`)
}

console.log("[validate] tsc --noLib --noEmit: OK")

// ─── 2. AST walk: enumerate namespaces, relations, permits ───────────────────

const source = program.getSourceFile(CONFIG_FILE)
if (!source) fail(`could not load source file: ${CONFIG_FILE}`)

interface NamespaceInfo {
  name: string
  relations: Array<{ name: string; types: string[] }>
  permits: string[]
}

const namespaces: NamespaceInfo[] = []

ts.forEachChild(source, (node) => {
  if (!ts.isClassDeclaration(node) || !node.name) return

  const implementsNamespace = (node.heritageClauses ?? []).some((clause) =>
    clause.token === ts.SyntaxKind.ImplementsKeyword &&
    clause.types.some((t) => t.expression.getText(source) === "Namespace"),
  )
  if (!implementsNamespace) return

  const info: NamespaceInfo = {
    name: node.name.text,
    relations: [],
    permits: [],
  }

  for (const member of node.members) {
    // related: { foo: Foo[]; bar: (User | SubjectSet<X, "y">)[] }
    if (
      ts.isPropertyDeclaration(member) &&
      member.name.getText(source) === "related" &&
      member.type &&
      ts.isTypeLiteralNode(member.type)
    ) {
      for (const sig of member.type.members) {
        if (
          ts.isPropertySignature(sig) &&
          sig.type &&
          ts.isIdentifier(sig.name)
        ) {
          info.relations.push({
            name: sig.name.text,
            types: [sig.type.getText(source)],
          })
        }
      }
    }

    // permits = { view: (ctx) => ..., edit: (ctx) => ... }
    if (
      ts.isPropertyDeclaration(member) &&
      member.name.getText(source) === "permits" &&
      member.initializer &&
      ts.isObjectLiteralExpression(member.initializer)
    ) {
      for (const prop of member.initializer.properties) {
        if (
          (ts.isPropertyAssignment(prop) ||
            ts.isMethodDeclaration(prop) ||
            ts.isShorthandPropertyAssignment(prop)) &&
          prop.name &&
          ts.isIdentifier(prop.name)
        ) {
          info.permits.push(prop.name.text)
        }
      }
    }
  }

  namespaces.push(info)
})

if (namespaces.length === 0) {
  fail("no Namespace classes found in config")
}

// ─── 3. Render the graph ────────────────────────────────────────────────────

console.log("")
console.log("[validate] permission graph")
console.log("─".repeat(60))
for (const ns of namespaces) {
  console.log(`namespace ${ns.name}`)
  if (ns.relations.length === 0) {
    console.log("  (no relations)")
  } else {
    for (const r of ns.relations) {
      console.log(`  relation ${r.name}: ${r.types.join(", ")}`)
    }
  }
  if (ns.permits.length > 0) {
    console.log(`  permits → ${ns.permits.join(", ")}`)
  }
  console.log("")
}

// ─── 4. Sanity: the four required namespaces are present ────────────────────

const required = ["KanbanProject", "KanbanBoard", "KanbanCard", "_kanban_health"]
const missing = required.filter((r) => !namespaces.some((n) => n.name === r))
if (missing.length > 0) {
  fail(`missing required namespaces: ${missing.join(", ")}`)
}

console.log(`[validate] all ${required.length} required namespaces present`)
console.log("[validate] OK")
