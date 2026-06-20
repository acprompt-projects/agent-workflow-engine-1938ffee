# Workflow DAG Schema & Execution Model

## 1. Overview

An **agent workflow** is a directed acyclic graph (DAG) where **nodes** represent task steps and **edges** represent dependencies. The engine performs a topological traversal, scheduling nodes whose dependencies are satisfied. Nodes may execute sequentially, in parallel, or conditionally based on runtime data.

## 2. Core Data Model

### 2.1 WorkflowDefinition

```json
{
  "id": "wf_01HXYZ",
  "name": "research-and-summarize",
  "version": "1.0.0",
  "inputs": { "topic": { "type": "string", "required": true } },
  "nodes": [ /* see 2.2 */ ],
  "edges": [ /* see 2.3 */ ],
  "metadata": { "author": "agent-orchestrator", "created": "2025-01-01T00:00:00Z" }
}
```

### 2.2 Node

Every node has a `type` that determines execution semantics.

```json
{
  "id": "node_research",
  "type": "task",
  "agent": "research-agent-v2",
  "action": "web_search",
  "inputs": { "query": "{{wf.inputs.topic}}" },
  "timeout_ms": 30000,
  "retries": 2,
  "on_failure": "fail" | "skip" | "fallback",
  "fallback": { "agent": "fallback-agent", "action": "cached_search" }
}
```

**Node Types:**

| Type | Purpose | Key Fields |
|------|---------|------------|
| `task` | Single actionable step delegated to an agent | `agent`, `action`, `inputs` |
| `parallel` | Fan-out: runs all children concurrently | `children` (node id list) |
| `join` | Fan-in: waits for all upstream nodes, then merges | `merge_strategy`: `"all"\|"any"\|"quorum"` |
| `conditional` | Branch selector: picks one outgoing path | `condition` (CEL expression) |
| `subgraph` | Nested workflow, enabling composition | `workflow_id` |

### 2.3 Edge

```json
{
  "source": "node_research",
  "target": "node_conditional",
  "label": "success",
  "port": "on_success"
}
```

Edges carry an optional `port` (`on_success`, `on_failure`, `on_skip`) that conditional and join nodes use to differentiate incoming data flows.

## 3. State Machine

Each **NodeInstance** (a node within a running workflow) transitions through states:

```
PENDING → READY → RUNNING → (SUCCEEDED | FAILED | SKIPPED)
                       ↑         |
                       └─ RETRY ─┘
```

| State | Meaning |
|-------|---------|
| `PENDING` | Dependencies not yet met |
| `READY` | All deps satisfied; awaiting scheduler |
| `RUNNING` | Agent is executing |
| `RETRY` | Transient failure; will re-run after backoff |
| `SUCCEEDED` | Completed with output |
| `FAILED` | Terminal failure (retries exhausted) |
| `SKIPPED` | Skipped by conditional branch or `on_failure: skip` |

The **WorkflowInstance** state is derived:
- `RUNNING` while any node is `READY`/`RUNNING`/`RETRY`
- `SUCCEEDED` when all terminal nodes are `SUCCEEDED` or `SKIPPED`
- `FAILED` when any terminal node is `FAILED` and no nodes remain runnable

## 4. Execution Semantics

1. **Scheduling**: On start, all nodes with zero in-degree become `READY`. The scheduler dispatches them (respecting `parallel` fan-out).
2. **Dependency Resolution**: When a node transitions to `SUCCEEDED`/`SKIPPED`/`FAILED`, the engine evaluates each downstream node: if **all** required predecessors are resolved, it becomes `READY`.
3. **Parallel Nodes**: A `parallel` node creates an implicit barrier — its children are all marked `READY` simultaneously. The matching `join` node waits per its `merge_strategy`.
4. **Conditional Nodes**: At runtime, the `condition` expression (evaluated against upstream outputs) selects exactly one outgoing edge; other branches produce no further scheduling.
5. **Retries**: On transient failure, the node enters `RETRY` with exponential backoff (`base_ms=500`, factor 2, capped by `timeout_ms`). After `retries` attempts, it becomes `FAILED`.
6. **Data Flow**: Each node's output is stored in the execution context. Downstream nodes reference outputs via `"{{nodes.node_id.output.<field>}}"` template expressions resolved at dispatch time.

## 5. JSON Schema

See `schema/workflow-dag-v1.json` for the formal validation schema.

### Minimal Example

```json
{
  "id": "wf_min",
  "name": "hello-pipeline",
  "version": "1.0.0",
  "inputs": {},
  "nodes": [
    { "id": "n1", "type": "task", "agent": "greeter", "action": "greet", "inputs": {}, "timeout_ms": 5000, "retries": 0, "on_failure": "fail" }
  ],
  "edges": []
}
```