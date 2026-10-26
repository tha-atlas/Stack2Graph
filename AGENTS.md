# Repository Instructions

## Scope and architecture

- Use Python 3.14 and UV. Keep the direct packages listed in `pyproject.toml`.
- Keep executable areas independent. Do not create a global CLI or shared all-purpose runner.
- Keep `start.py` thin. Put reusable processing APIs in their area so notebooks can call them directly.
- Pass dependencies explicitly. Use dataclasses for data and configuration when needed.
- Use small Protocol interfaces for real adapter consumers. Prefer batch getters and streaming to queries per record.
- SQL, database connections, and transactions belong in `adapter/`. Never create global database connections.
- Use classes when state, resources, or dependencies justify them. Use functions for stateless processing.
- Add only abstractions needed by current behavior. Do not add migrations, legacy implementations, or compatibility wrappers.
- Keep domain helpers in their area. Put helpers in `utils/` only when multiple areas use them.
- PostgreSQL 18 with Psycopg 3 is the dump database foundation. Keep future GPU dependencies optional.
- Store large data in a configurable directory outside the repository. Examples must use placeholders, not private deployment values.
- Write code, comments, and documentation in clear English.

## Environment configuration

- Application entry points and scripts must automatically load the repository-root `.env` unless an explicit alternative is supplied. Resolve the default path from the repository location, not the current working directory.
- The `--env-file` parameter must always be optional and default to the repository-root `.env`. Users must not need to specify it for normal execution.
- Omit `--env-file .env` from normal usage examples; show the parameter only when demonstrating an explicit alternative file.
- PostgreSQL, Qdrant, QLever and gateway deployment commands are exceptions: preserve their service-specific environment configuration under their respective `deployment/` directories.

## Typing and code quality

- Apply these requirements to all project Python code, including tests, scripts, and reusable notebook APIs.
- Use precise types throughout. Annotate every function and method, including parameters and return values, and fully parameterize collections and other generic types.
- Do not use `Any`, unparameterized generics, or broad types such as `object` to bypass a known domain type. Use concrete types, typed unions, `TypedDict`, dataclasses, or small Protocol interfaces where appropriate.
- For every third-party library, use its bundled type information when available. Otherwise, add and install a compatible type-stub package when one exists, recording it as a development dependency in `pyproject.toml` and updating the UV lockfile through the normal dependency workflow. If no stubs exist, provide precise local stubs for the APIs used.
- Keep mypy strict mode enabled and reject explicit or implicit `Any`, including types introduced by untyped imports. Strict mode alone does not reject every use of `Any`; enable the additional `disallow_any_*` checks needed to enforce this rule.
- Do not hide typing or lint errors with `type: ignore`, `noqa`, `ignore_missing_imports`, disabled checks, or casts that merely silence an error. Fix the underlying types and code instead.
- Run `ruff check .`, `ruff format --check .`, and `mypy --strict .` from the repository root using the project environment before declaring code changes complete. All project code must pass Ruff and produce zero mypy errors; checking only changed files is insufficient.
- If the required tools or type packages are unavailable, report the missing prerequisites and mark those checks as unverified. Preserve the scaffold restrictions below: do not install packages during scaffold-only work.

## Scaffold safety and verification

- Preserve existing files and inspect repository instructions before editing.
- Unimplemented workflows must fail explicitly. Help must work without external resources.
- Scaffold-only paths must stay usable without optional runtime dependencies.
- Do not install packages or Python, run `uv sync`, `uv add`, or `pip install`, or fabricate `uv.lock` as part of scaffold work.
- Do not download production data or run GPU jobs during scaffold work.
- Authorized PostgreSQL implementation may use UV dependencies, Docker Compose and isolated integration servers; other running services must stay untouched.
- Always remove temporary integration-test containers, volumes, networks, data, certificates, credentials and scratch environments after testing, including failed or interrupted test runs. Use ownership checks and cleanup in `finally`; preserve production services, shared runtime dependencies, source fixtures and durable verification reports. Never leave test services running for later reuse after completing the task.
- Do not commit or push without an explicit request.
- Verify with an available interpreter: `python -B -m unittest discover -s tests -v`.
- If Python 3.14 is unavailable, install nothing and report the verification limitation.

## Current documentation with Context7

Use Context7 MCP whenever a task asks about a library, framework, SDK, API,
CLI tool, or cloud service. This includes syntax, configuration, migrations,
library-specific debugging, setup, and CLI usage, even for familiar tools.
Prefer Context7 over web search for library documentation.

1. Call `resolve-library-id` with the library name and documentation question, unless the user supplies an exact `/org/project` ID.
2. Select by exact name, relevance, snippet coverage, source reputation, and benchmark score. Use version-specific IDs when requested. Retry with an alternate name or query when results do not match.
3. Call `query-docs` for one concept at a time. Separate distinct concepts unless the question concerns their interaction.
4. Base the answer on the fetched documentation.

Do not use Context7 for refactoring, writing scripts from scratch, business-logic
debugging, code review, or general programming concepts.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **Stack2Graph2** (2275 symbols, 6024 relationships, 181 execution flows).

> Index stale? Run `node .gitnexus/run.cjs analyze --index-only` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? Bootstrap with `npx`, `bunx`, or `pnpm dlx` — e.g. `bunx gitnexus@latest analyze` (npm 11 npx crash; #1939).

## Always Do

- **MUST run impact before editing.** Use `impact({target: "symbolName", direction: "upstream"})` or `node .gitnexus/run.cjs impact "symbolName" --direction upstream --repo .`; report callers, processes, and risk. Never substitute grep for graph analysis.
- **MUST analyze graph changes before committing.** Use `detect_changes({scope: "all"})` (MCP) or `node .gitnexus/run.cjs detect-changes --scope all --repo .` (CLI fallback). `partial: true` or `truncated: true` is not a clean check — a zero means unseen, not unaffected; re-run it. For regression review: `detect_changes({scope: "compare", base_ref: "main"})` or `node .gitnexus/run.cjs detect-changes --scope compare --base-ref "main" --repo .`.
- MUST warn on HIGH/CRITICAL `risk` pre-edit; never use `riskSharedAxes` to waive a HIGH/CRITICAL `risk` warning. Compare File/symbol: MCP File omits axes; Graph-RAG expands File.
- **MUST treat `risk: UNKNOWN` as unresolved, not as low.** An empty caller set is not evidence the symbol is unused — it can also mean the callers are not resolvable by the index (plain-object property access, dynamic dispatch, cross-language calls). `impact` pairs `UNKNOWN` with a `riskNote` saying so. Confirm with a text search before treating the symbol as safe to change or delete; do not proceed on the strength of a zero.
- **MUST use `query({search_query: "concept"})` for concepts/flows, `context({name: "symbolName"})` for a named symbol, or `impact` for blast radius, on read-only callers, dependencies, imports, or execution flow.** Graph first; text search only for empty/`UNKNOWN`/literals.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method before MCP/CLI impact analysis.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis, and never read `UNKNOWN` as an all-clear — it means the walk could not answer, which is the one verdict that requires confirming by other means.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit before MCP/CLI graph change analysis.

## Resources

| Resource | Use for |
| --- | --- |
| `gitnexus://repo/Stack2Graph2/context` | Codebase overview, check index freshness |
| `gitnexus://repo/Stack2Graph2/clusters` | All functional areas |
| `gitnexus://repo/Stack2Graph2/processes` | All execution flows |
| `gitnexus://repo/Stack2Graph2/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
| --- | --- |
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
