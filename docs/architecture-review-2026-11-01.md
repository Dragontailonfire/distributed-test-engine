# Architecture & Specification Evaluation

* **Date:** 2026-11-01  
* **Target:** `README.md` (Specification & Architecture Guide)  
* **Status:** Architectural Review & Actionable Recommendations  
* **Author / Reviewer:** Antigravity AI Engine Pair Review  

---

## 1. Executive Summary

The specification outlined in `README.md` presents a robust, production-grade vision for a distributed test engine. The primary strengths include:
* Using **LPT (Longest Processing Time)** bin-packing with **P90 historical durations** to eliminate worker stragglers.
* Leveraging **`tmpfs` (RAM disk)** and PostgreSQL **`TEMPLATE` cloning** with disabled durability flags (`fsync=off`, `synchronous_commit=off`) for sub-second hermetic state resets.
* Employing **Weighted Fair Polling (70/20/10)** to prevent queue starvation across priority lanes.
* A phased milestone roadmap following Spec-Driven Development (SDD).

However, critical technical blind spots and contract omissions were identified that would cause runtime failures or architectural bottlenecks during implementation if unaddressed. This document catalogs all identified concerns and actionable solutions.

---

## 2. Identified Concerns & Technical Blind Spots

### 2.1 Telemetry Feedback Loop Omission (P90 Historical Data Starvation)
* **Location:** `README.md` § 5 (`ShardResultSchema`)
* **Problem:** `ShardResultSchema` records overall shard duration and an array of `failedTests`:
  ```typescript
  export const ShardResultSchema = z.object({
    shardId: z.string().uuid(),
    buildId: z.string(),
    workerId: z.string(),
    status: z.enum(['PASSED', 'FAILED', 'ABORTED']),
    actualDurationMs: z.number().int(),
    failedTests: z.array(
      z.object({
        testId: z.string(),
        errorMessage: z.string(),
        durationMs: z.number().int(),
      })
    ),
  });
  ```
  It **omits execution durations for passing tests**.
* **Impact:** Milestone 1 depends on historical P90 durations for every test item. Without telemetry from passed runs, the historical database cannot be refreshed as test execution profiles shift over time.
* **Suggestion:** Add a complete test outcome telemetry array (or map) in `ShardResultSchema`:
  ```typescript
  export const TestExecutionResultSchema = z.object({
    testId: z.string(),
    durationMs: z.number().int().nonnegative(),
    status: z.enum(['PASSED', 'FAILED', 'SKIPPED']),
    errorMessage: z.string().optional(),
    stackTrace: z.string().optional(),
  });

  export const ShardResultSchema = z.object({
    shardId: z.string().uuid(),
    buildId: z.string(),
    workerId: z.string(),
    status: z.enum(['PASSED', 'FAILED', 'ABORTED']),
    actualDurationMs: z.number().int(),
    tests: z.array(TestExecutionResultSchema),
  });
  ```

---

### 2.2 Process Warmth vs. `execa` Child-Process Isolation Contradiction
* **Location:** `README.md` § 2 & § 4.4
* **Problem:** The specification states: *"Spawns isolated test runners while keeping heavy dependencies (headless browsers, DB connections) warm."*
* **Impact:** Spawning an external test runner via `execa` (`node:child_process.spawn`) creates an isolated OS process. A child process **cannot inherit open TCP connections, PostgreSQL connection pool handles, or active DevTools Protocol (CDP) WebSocket sessions** from the parent Node daemon without a local proxy or client-server socket interface. Furthermore, each `execa` invocation incurs Node.js bootstrap overhead (500ms–2,000ms).
* **Suggestions:**
  1. **PostgreSQL Warmth:** The worker daemon performs the `TEMPLATE` clone in RAM, then passes the newly generated database connection string (`DATABASE_URL=postgres://runner:.../active_test_db_${workerId}`) to the child runner's environment variables.
  2. **Headless Browser Warmth:** Run a persistent Chromium/Playwright browser server as a long-lived daemon within the worker container (`npx playwright run-server --port 9222`), passing `BROWSER_WS_ENDPOINT` to the child process. The child runner connects over local WebSocket instead of launching a fresh Chromium instance per shard.

---

### 2.3 Single Tests Exceeding the 30-Second Target Chunk Size
* **Location:** `README.md` § 4.1
* **Problem:** The LPT algorithm targets dynamic bins capped at ~30,000ms. In real-world WebUI and heavy integration suites, individual test files or end-to-end user journeys regularly take 45–120 seconds.
* **Impact:** LPT cannot pack a 60-second test into a 30-second bin without exceeding the cap. If bin-packing opens a new shard, that shard immediately becomes an unavoidable straggler.
* **Suggestions:**
  1. **Test Granularity Definition:** Explicitly define whether `TestItemSchema.testId` refers to a test *file* or an individual test *case* (e.g. `filePath::describe::testName`).
  2. **Oversized Item Handler:** If a single test has `p90DurationMs > 30,000`:
     * Allocate it to a dedicated solo shard with a warning flag.
     * Provide tooling/heuristics to split large test files into individual test-case invocations (e.g., using test framework grep filters like `vitest -t` or `playwright --grep`).

---

### 2.4 Heartbeat Timing vs. Worker Event-Loop Starvation & Split-Brain Risks
* **Location:** `README.md` § 4.2
* **Problem:** The queue visibility timeout is **15 seconds**, and the worker heartbeat interval is **5 seconds**.
* **Impact:** Node.js runs on a single-threaded event loop. If the worker daemon experiences CPU saturation, synchronous AST parsing, heavy compilation, or child process spawning delays, a 5-second `setInterval` can easily drift beyond 15 seconds. If the visibility timeout lapses while the worker is actively executing the shard, Redis un-hides the task, a second worker picks it up, and duplicate executions occur simultaneously (split-brain race condition).
* **Suggestions:**
  1. Increase the visibility timeout safety margin (e.g., **30-second visibility timeout** with **5-second heartbeats**).
  2. Decouple heartbeats from the main event loop by running heartbeat timers in a separate `node:worker_threads` instance or sidecar.
  3. Implement explicit shard leasing/fencing tokens in Redis so stale workers abort if their lease expires before shard submission.

---

### 2.5 Database Name Collision in Multi-Worker Concurrency
* **Location:** `README.md` § 4.4
* **Problem:** The reset script uses a hardcoded database name:
  ```sql
  DROP DATABASE IF EXISTS active_test_db WITH (FORCE);
  CREATE DATABASE active_test_db TEMPLATE base_template_db;
  ```
* **Impact:** When multiple worker processes share a single PostgreSQL container (e.g., in `compose.yaml` or multi-worker Pods), workers will concurrently drop and recreate `active_test_db`, corrupting each other's test runs.
* **Suggestion:** Scope the active database dynamically per worker instance:
  ```sql
  DROP DATABASE IF EXISTS active_test_db_${workerId} WITH (FORCE);
  CREATE DATABASE active_test_db_${workerId} TEMPLATE base_template_db;
  ```

---

### 2.6 PostgreSQL `TEMPLATE` Cloning Concurrency & Migration Drift
* **Location:** `README.md` § 4.4 & § 6
* **Problem:**
  1. PostgreSQL allows concurrent cloning from a template database, but **no active connections** can be writing to or holding exclusive locks on `base_template_db` during clone operations.
  2. In feature branches with schema changes, new database migrations need to be applied before tests run.
* **Suggestions:**
  1. Set `base_template_db` to read-only (`ALTER DATABASE base_template_db is_template true;`) once initial migrations complete, preventing lingering open writes from blocking clones.
  2. In the worker bootstrap sequence, ensure workers check the current Git commit hash and run pending migrations against `base_template_db` once per build before processing shards.

---

### 2.7 Static Worker Pool Allocation vs. Dynamic Workload Inefficiency
* **Location:** `README.md` § 1 (Blueprint) & § 4.3
* **Problem:** Workers are partitioned into rigid percentage pools:
  * Unit: 8%
  * API: 30%
  * WebUI: 53%
  * Mobile: 8.5%
* **Impact:** If a commit only touches backend unit or API code, the WebUI and Mobile queues remain empty, leaving **61.5% of cluster resources idle** while API tests queue up.
* **Suggestion:** Implement **Work-Stealing / Cross-Queue Fallback**:
  * If a worker's primary queue (e.g. WebUI) is empty, allow it to pull from API or Unit queues (provided container dependencies like Node/DB are compatible).
  * Reserve dedicated hardware only for suites with unique container prerequisites (e.g., Mobile emulators).

---

### 2.8 Cold Start Strategy for New / Modified Tests
* **Location:** `README.md` § 4.1
* **Problem:** Newly introduced tests or significantly refactored tests lack historical execution data.
* **Impact:** If `p90DurationMs` defaults to 0, LPT treats new tests as tiny "pebbles" and clumps them into shards, potentially creating large runtime variances and straggler shards.
* **Suggestion:** Define a conservative cold-start duration fallback policy:
  * For new tests, assign the **median (P50)** duration of existing tests in the same suite, or a suite-specific baseline (e.g., Unit: 500ms, API: 2,500ms, WebUI: 10,000ms).
  * Mark cold-start tests with a `isColdStart: true` flag in `TestItemSchema` to prioritize them for early duration recording.

---

## 3. Recommended Contract Updates (`packages/shared-types`)

To address the findings above, the following adjustments to the shared data contracts are recommended:

```typescript
import { z } from 'zod';

export const SuiteTypeSchema = z.enum(['UNIT', 'API', 'WEBUI', 'MOBILE']);
export const PriorityLaneSchema = z.enum(['P0_FAST', 'P1_STANDARD', 'P2_BULK']);
export const TestStatusSchema = z.enum(['PASSED', 'FAILED', 'SKIPPED']);

export const TestItemSchema = z.object({
  testId: z.string(),
  filePath: z.string(),
  testNameFilter: z.string().optional(), // Enables test-case level splitting
  suite: SuiteTypeSchema,
  p90DurationMs: z.number().int().nonnegative(),
  isColdStart: z.boolean().default(false),
});

export const ShardManifestSchema = z.object({
  shardId: z.string().uuid(),
  buildId: z.string(),
  commitHash: z.string(),
  suite: SuiteTypeSchema,
  priority: PriorityLaneSchema,
  estimatedDurationMs: z.number().int().nonnegative(),
  retryCount: z.number().int().default(0),
  tests: z.array(TestItemSchema),
});

export const WorkerHeartbeatSchema = z.object({
  workerId: z.string(),
  shardId: z.string().uuid(),
  timestamp: z.number(),
  memoryUsageMb: z.number(),
  fencingToken: z.number().int(),
});

export const SingleTestResultSchema = z.object({
  testId: z.string(),
  status: TestStatusSchema,
  durationMs: z.number().int().nonnegative(),
  errorMessage: z.string().optional(),
  stackTrace: z.string().optional(),
});

export const ShardResultSchema = z.object({
  shardId: z.string().uuid(),
  buildId: z.string(),
  workerId: z.string(),
  status: z.enum(['PASSED', 'FAILED', 'ABORTED']),
  actualDurationMs: z.number().int().nonnegative(),
  testResults: z.array(SingleTestResultSchema), // Full telemetry for P90 updates
});
```

---

## 4. Implementation Phasing Recommendations

* **Milestone 1 (Shared Contracts & LPT Shard Planner):**
  * Include the updated `ShardResultSchema` and `TestItemSchema`.
  * Implement the cold-start fallback heuristic and the oversized test (>30s) partitioner.
* **Milestone 2 (TIA Engine):**
  * Ensure `ts-morph` AST traversal accounts for path aliases (`tsconfig.json` `compilerOptions.paths`) and monorepo package imports.
* **Milestone 3 (Priority Queue & Scheduler):**
  * Extend visibility timeout to 30s with 5s heartbeat interval; introduce lease fencing tokens in Redis.
* **Milestone 4 (Hermetic Warm Worker):**
  * Implement dynamic database naming (`active_test_db_${workerId}`).
  * Decouple browser server (persistent CDP endpoint) from child process test runners.
* **Milestone 5 (Central Aggregator & Orchestration):**
  * Store per-test timing metrics upon shard completion to compute rolling P90 historical values.
  * Implement cross-queue work stealing policy across idle workers.
