# distributed-test-engine
High-throughput, low-latency distributed test execution engine capable of running massive test suites with minimal infrastructure overhead and sub-minute developer feedback loops.

# Specification & Architecture Guide

> **Development Paradigm:** Spec-Driven Development (SDD)  
> **Primary Tech Stack:** TypeScript (Node.js), `pnpm` Monorepo, Podman, Redis, PostgreSQL (`tmpfs`)  
> **Target Goal:** Execute thousands of automated tests across Unit, API, WebUI, and Mobile suites in ~30-second deterministic windows using intelligent filtering, bin-packing, and hermetic warm workers.

---

## 1. System Architecture Blueprint

```text
                [ Developer Git Push / Merge Queue ]
                                 │
                                 ▼
                     [ Commit Ingestion & TIA ] 
                (Git Diff vs. ts-morph AST / Coverage Graph)
                                 │
                                 ▼
                       [ Shard Planner Engine ] 
                 (P90 duration LPT bin-packing -> ~30s chunks)
                                 │
      ┌──────────────────────────┼──────────────────────────┬──────────────────────────┐
      ▼                          ▼                          ▼                          ▼
[Unit Queue]                [API Queue]               [WebUI Queue]              [Mobile Queue]
(Lanes: P0, P1, P2)       (Lanes: P0, P1, P2)       (Lanes: P0, P1, P2)        (Lanes: P0, P1, P2)
      │                          │                          │                          │
      ▼                          ▼                          ▼                          ▼
  [8% Pool]                 [30% Pool]                 [53% Pool]                 [8.5% Pool]
  (Podman Warm Workers — Long-Polling via Weighted Fair Scheduling + tmpfs Postgres Clones)
      │                          │                          │                          │
      └──────────────────────────┴─────────────┬────────────┴──────────────────────────┘
                                               │
                                               ▼ (Redis Streams / gRPC)
                                    [ Central Aggregator ] 
                             (Real-time Hono API & PR Status Checks)

```

---

## 2. Finalized Technology Stack

| Layer / Component | Chosen Technology | Role in Architecture |
| --- | --- | --- |
| **Language & Runtime** | **TypeScript 5.x + Node.js (LTS)** | Non-blocking async I/O for queue polling, heartbeat timers, child process orchestration, and shared schema safety. |
| **Monorepo Manager** | **`pnpm` Workspaces** | Manages shared TypeScript contracts (`zod`) across the Planner, TIA, Scheduler, Worker, and Aggregator packages. |
| **Schema & Contracts** | **`zod`** | Runtime validation and static TypeScript inference for Shard Manifests, Heartbeats, and Test Results. |
| **AST & Static Analysis** | **`ts-morph`** (or `@swc/core`) | Parses source files to build the reverse dependency graph for Test Impact Analysis (TIA). |
| **Queue & Scheduling** | **Redis + `ioredis` / `bullmq**` | Powers the P0/P1/P2 priority queues, visibility timeouts, heartbeat locks, and Dead-Letter Queues (DLQ). |
| **Database & Hermetic State** | **PostgreSQL 16 + `pg**` | Runs with data directory mounted on Linux RAM-disk (`tmpfs`) inside Podman for ~20ms `TEMPLATE` database cloning. |
| **Worker Process Runner** | **`execa` (`node:child_process`)** | Spawns isolated test runners while keeping heavy dependencies (headless browsers, DB connections) warm. |
| **Aggregator & API** | **`hono`** | Lightweight, high-speed HTTP/SSE server for receiving worker telemetry and reporting PR status checks. |
| **Container Engine** | **Podman + `podman-compose**` | Rootless, daemonless container orchestration for local worker pools, Redis, and RAM-backed Postgres instances. |
| **Cloud / K8s Bridge** | **`podman kube play` -> Kubernetes + KEDA** | Allows testing Kubernetes Pod YAML (`emptyDir: Memory`) directly in Podman before deploying to a full K8s cluster. |

---

## 3. Monorepo Directory Structure

```text
distributed-test-engine/
├── package.json
├── pnpm-workspace.yaml
├── tsconfig.base.json
├── compose.yaml                # Podman Compose file (Redis, Postgres with tmpfs, Worker replicas)
├── pods/                       # Kubernetes Pod YAML specs (runnable via `podman kube play`)
│   └── worker-pod.yaml
└── packages/
    ├── shared-types/           # Zod schemas & TypeScript interfaces (Data Contracts)
    ├── shard-planner/          # Milestone 1: P90 runtime calculator & LPT bin-packing algorithm
    ├── tia-engine/             # Milestone 2: Git diff parser + ts-morph AST dependency graph
    ├── queue-scheduler/        # Milestone 3: Priority lanes (P0/P1/P2), WFQ poller, Visibility Timeouts & DLQ
    ├── worker-daemon/          # Milestone 4: Warm worker agent, CAS code mount, Postgres tmpfs template reset
    └── central-aggregator/     # Milestone 5: Hono telemetry server, shard completion tracker, PR reporter

```

---

## 4. Core Algorithms & Engineering Concepts (Learning Guide)

### 4.1 Sharding Strategy: LPT (Longest Processing Time) Bin-Packing

* **The Problem:** If you split 100 tests across 4 workers purely by count (25 tests each), Worker 1 might get 25 fast unit tests (total 5 seconds) while Worker 2 gets 25 heavy E2E tests (total 4 minutes). Your CI pipeline is always as slow as your slowest "straggler" worker.
* **Why P90 Duration?** Average (mean) test times are easily skewed by one-off network hiccups, while median (P50) ignores realistic slowdowns. **P90** (the duration that 90% of historical runs completed within) gives a reliable upper-bound estimate for planning.
* **How the LPT Algorithm Works:**
1. Fetch the historical P90 duration for every selected test.
2. Sort all tests in **descending order** (longest tests first).
3. Initialize $N$ empty shards (or dynamically open bins targeting a **30,000ms (30s)** cap).
4. Iterate through the sorted tests and place each test into the shard that currently has the **lowest total accumulated duration** ( commonly implemented using a Min-Heap or sorted array).

* **Why Sort Longest First?** Packing the big "boulders" first and filling the gaps at the end with tiny 100ms "pebbles" guarantees all shards finish at almost the exact same time.

### 4.2 Scheduling Model: Pull-Based Queues, Visibility Timeouts & DLQ

* **Push vs. Pull:** If a central coordinator *pushes* tests to workers, it can easily overwhelm a slow worker or send tasks to a dead container. In a **Pull Model**, workers ask for work only when they are idle and healthy.
* **Visibility Timeout & Heartbeats:**
1. Worker pulls `shard-101` from the queue. The queue hides `shard-101` from other workers for **15 seconds** (Visibility Timeout).
2. While running the 30-second shard, the Node.js worker sends a `HEARTBEAT` every **5 seconds**, resetting the 15-second countdown.
3. If the Podman container crashes (e.g., Out-Of-Memory), the heartbeats stop. After 15 seconds, the queue automatically makes `shard-101` visible again so another healthy worker can pull it.

* **Poison-Pill Isolation (Dead-Letter Queue):** What if a specific test triggers a segfault that crashes *every* worker that pulls it? Each time a shard times out without completing, its `retryCount` increments. Once `retryCount > 3`, the shard is moved to the **DLQ** so it stops killing the worker pool, and the developer is alerted.

### 4.3 Priority & Starvation Prevention: Weighted Fair Polling

* **The Problem:** Strict priority (`P0 Fast PRs` always before `P1 Standard` and `P2 Nightly Bulk`) causes **queue starvation**—if developers push PRs non-stop all afternoon, a P2 release test queued at 1:00 PM might never run.
* **The Solution (Weighted Fair Polling):** When a worker finishes a shard and polls for the next one, it uses a weighted probability roll (or deficit round-robin counter):
* **70% of polls** check `P0 -> P1 -> P2`
* **20% of polls** check `P1 -> P0 -> P2`
* **10% of polls** check `P2 -> P1 -> P0`
* * Note:* If the chosen priority lane is empty, the worker immediately falls back to the other lanes so zero compute sits idle.

### 4.4 Hermetic Warm Workers & 20ms Database Resets

* **Cold vs. Warm:** Spinning up a new container, booting a database, running migrations, and launching Chromium takes 20–45 seconds—longer than the tests themselves! **Warm workers** stay running across jobs.
* **The Dirty State Problem:** If Worker A runs Test 1 (which inserts a user `alice@example.com`), and then runs Test 2, Test 2 might fail because `alice@example.com` already exists.
* **The `tmpfs` + Postgres `TEMPLATE` Solution:**
1. In Podman, we mount the PostgreSQL data directory directly to a Linux RAM-disk (`--tmpfs /var/lib/postgresql/data:rw,size=512m`). Everything lives in system memory—zero disk I/O wait.
2. When the worker boots, it runs all DB migrations *once* on a master database named `base_template_db`.
3. Before each test or micro-shard, the worker runs:
```sql
DROP DATABASE IF EXISTS active_test_db WITH (FORCE);
CREATE DATABASE active_test_db TEMPLATE base_template_db;

```

4. Because `base_template_db` resides in RAM (`tmpfs`), PostgreSQL performs an in-memory file copy of the pre-migrated database in **~15 to 25 milliseconds**. Every test gets a 100% clean, isolated ("hermetic") database at near-zero cost.

### 4.5 Test Impact Analysis (TIA): Hybrid Static + Dynamic Graph

* **Static Layer (Unit & Integration):** Uses `ts-morph` to parse `import` statements into a Directed Acyclic Graph (DAG). If `git diff` shows `src/utils/tax.ts` changed, we traverse the reverse edges of the graph to find `tests/unit/billing.test.ts` and `tests/api/checkout.spec.ts`.
* **Dynamic Layer (WebUI / E2E):** E2E tests often hit backend APIs over HTTP where static TypeScript imports don't connect the test file to the backend code. During main-branch builds, we record runtime code coverage per test and store a mapping: `File X -> [E2E Test A, E2E Test B]`.
* **Safety Net:** TIA runs on PRs for speed, while the **Merge Queue** runs 100% of the test suite to guarantee zero false negatives slip into production.

---

## 5. Shared Data Contracts (`packages/shared-types`)

All services communicate using strict `zod` schemas:

```typescript
import { z } from 'zod';

export const SuiteTypeSchema = z.enum(['UNIT', 'API', 'WEBUI', 'MOBILE']);
export const PriorityLaneSchema = z.enum(['P0_FAST', 'P1_STANDARD', 'P2_BULK']);

export const TestItemSchema = z.object({
  testId: z.string(),
  filePath: z.string(),
  suite: SuiteTypeSchema,
  p90DurationMs: z.number().int().nonnegative(),
});

export const ShardManifestSchema = z.object({
  shardId: z.string().uuid(),
  buildId: z.string(),
  commitHash: z.string(),
  suite: SuiteTypeSchema,
  priority: PriorityLaneSchema,
  estimatedDurationMs: z.number().int(),
  retryCount: z.number().int().default(0),
  tests: z.array(TestItemSchema),
});

export const WorkerHeartbeatSchema = z.object({
  workerId: z.string(),
  shardId: z.string().uuid(),
  timestamp: z.number(),
  memoryUsageMb: z.number(),
});

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

---

## 6. Podman Infrastructure Configuration

### Local Development (`compose.yaml`)

Works out of the box with `podman compose up -d`:

```yaml
version: '3.8'

services:
  redis-queue:
    image: docker.io/library/redis:7-alpine
    ports:
      - "6379:6379"

  postgres-tmpfs:
    image: docker.io/library/postgres:16-alpine
    environment:
      POSTGRES_USER: runner
      POSTGRES_PASSWORD: runner_password
      POSTGRES_DB: base_template_db
    ports:
      - "5432:5432"
    # Mounts Postgres storage directly into RAM for ~20ms TEMPLATE cloning
    tmpfs:
      - /var/lib/postgresql/data:rw,size=512m
    command: >
      postgres
      -c fsync=off
      -c synchronous_commit=off
      -c full_page_writes=off

```

*(Note: Turning off `fsync` and `synchronous_commit` on a test-only `tmpfs` DB is safe because ephemeral test data does not need power-loss crash recovery, boosting speed even further.)*

---

## 7. Phased Implementation Roadmap (Milestones)

### Milestone 1: Shared Contracts & The LPT Shard Planner

* **Goal:** Set up the `pnpm` monorepo, define the `zod` data contracts in `packages/shared-types`, and implement the LPT (Longest Processing Time) bin-packing algorithm in `packages/shard-planner`.
* **Deliverable:** A CLI/module that ingests a mock dataset of 500 tests with varying historical runtimes, computes P90 durations, packs them into balanced ~30s shards, and prints a shard variance report proving no stragglers exist.

### Milestone 2: Test Impact Analysis (TIA) Engine

* **Goal:** Build `packages/tia-engine` using `ts-morph` and `git diff`.
* **Deliverable:** Given a mock target repo and a modified TypeScript file, traverse the AST import graph + JSON coverage map to output only the subset of impacted `TestItem` objects to feed into Milestone 1's Shard Planner.

### Milestone 3: Priority Queue Scheduler & Fault Tolerance

* **Goal:** Spin up Redis in Podman and implement `packages/queue-scheduler`.
* **Deliverable:** Implement the P0/P1/P2 priority queues, the 70/20/10 Weighted Fair Polling selector, the 5s Heartbeat / 15s Visibility Timeout monitor, and automatic Dead-Letter Queue (DLQ) routing after 3 simulated worker crashes.

### Milestone 4: Hermetic Warm Worker & `tmpfs` Postgres Reset

* **Goal:** Spin up `postgres-tmpfs` in Podman and build `packages/worker-daemon`.
* **Deliverable:** A long-running Node.js worker that pulls shards from Milestone 3, clones `base_template_db` -> `active_test_db` in ~20ms, executes the test files via `execa`, emits 5s heartbeats, and cleans up.

### Milestone 5: Central Aggregator & `podman kube play` Scaling

* **Goal:** Build `packages/central-aggregator` with `hono` to collect shard results and determine overall build pass/fail, then package the worker and DB sidecar into a Kubernetes Pod YAML (`pods/worker-pod.yaml`).
* **Deliverable:** Run `podman kube play pods/worker-pod.yaml` to validate Kubernetes-native `emptyDir: { medium: Memory }` mounts and end-to-end distributed execution.
