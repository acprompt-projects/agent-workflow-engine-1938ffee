===
import * as yaml from "js-yaml";

// --- Types ---

export interface StepDef {
  id: string;
  type: "task" | "branch" | "delegate" | "parallel" | "join";
  handler?: string;
  agentRef?: string;
  inputs?: Record<string, unknown>;
  dependsOn?: string[];
  condition?: { expr: string; then?: string; else?: string };
  timeout?: number;
  retryPolicy?: { maxRetries: number; backoffMs: number };
}

export interface WorkflowDef {
  name: string;
  version?: string;
  description?: string;
  steps: StepDef[];
}

export interface ExecNode {
  id: string;
  step: StepDef;
  dependencies: ExecNode[];
  dependents: ExecNode[];
}

export interface ExecutionGraph {
  name: string;
  version: string;
  entryNodes: ExecNode[];
  exitNodes: ExecNode[];
  nodes: Map<string, ExecNode>;
}

export interface ValidationError {
  stepId?: string;
  message: string;
  code: "duplicate_id" | "invalid_ref" | "cycle" | "type_mismatch" | "orphan" | "missing_field" | "no_entry";
}

// --- Parser ---

export function parseDefinition(raw: string, format?: "json" | "yaml"): WorkflowDef {
  const fmt = format ?? (raw.trimStart().startsWith("{") ? "json" : "yaml");
  const parsed = fmt === "json" ? JSON.parse(raw) : yaml.load(raw);
  if (!parsed || typeof parsed !== "object") throw new Error("Invalid definition: not an object");
  return parsed as WorkflowDef;
}

// --- Validator ---

export function validate(def: WorkflowDef): ValidationError[] {
  const errors: ValidationError[] = [];
  const ids = new Set<string>();

  if (!def.name) errors.push({ message: "Workflow name is required", code: "missing_field" });

  for (const step of def.steps) {
    if (!step.id) { errors.push({ message: "Step missing id", code: "missing_field" }); continue; }
    if (ids.has(step.id)) { errors.push({ stepId: step.id, message: `Duplicate step id: ${step.id}`, code: "duplicate_id" }); }
    ids.add(step.id);

    if (!step.type) errors.push({ stepId: step.id, message: `Step ${step.id} missing type`, code: "missing_field" });
    const validTypes = ["task", "branch", "delegate", "parallel", "join"];
    if (step.type && !validTypes.includes(step.type))
      errors.push({ stepId: step.id, message: `Invalid type '${step.type}'`, code: "type_mismatch" });

    if (step.type === "delegate" && !step.agentRef)
      errors.push({ stepId: step.id, message: "Delegate step requires agentRef", code: "missing_field" });

    if (step.dependsOn) {
      for (const dep of step.dependsOn) {
        if (!ids.has(dep)) errors.push({ stepId: step.id, message: `Unknown dependsOn ref: ${dep}`, code: "invalid_ref" });
      }
    }
  }

  // Cycle detection via DFS
  const stepMap = new Map(def.steps.filter(s => s.id).map(s => [s.id, s]));
  const WHITE = 0, GRAY = 1, BLACK = 2;
  const color = new Map<string, number>();
  for (const id of ids) color.set(id, WHITE);

  function dfs(id: string): boolean {
    color.set(id, GRAY);
    const step = stepMap.get(id);
    if (step?.dependsOn) {
      for (const dep of step.dependsOn) {
        if (color.get(dep) === GRAY) return true; // cycle
        if (color.get(dep) === WHITE && dfs(dep)) return true;
      }
    }
    color.set(id, BLACK);
    return false;
  }

  for (const id of ids) {
    if (color.get(id) === WHITE && dfs(id)) {
      errors.push({ stepId: id, message: `Cycle detected involving step: ${id}`, code: "cycle" });
      break;
    }
  }

  // Entry & orphan checks
  const hasDep = new Set(def.steps.flatMap(s => s.dependsOn ?? []));
  const entrySteps = def.steps.filter(s => s.id && !(s.dependsOn?.length));
  if (entrySteps.length === 0 && def.steps.length > 0)
    errors.push({ message: "No entry point (all steps have dependencies)", code: "no_entry" });

  const referenced = new Set([...hasDep, ...entrySteps.map(s => s.id!)]);
  for (const step of def.steps) {
    if (step.id && !referenced.has(step.id) && hasDep.size > 0)
      errors.push({ stepId: step.id, message: `Orphan step: ${step.id}`, code: "orphan" });
  }

  return errors;
}

// --- Graph Builder ---

export function buildExecutionGraph(def: WorkflowDef): ExecutionGraph {
  const errs = validate(def);
  if (errs.length > 0) throw new Error(`Validation failed: ${errs.map(e => e.message).join("; ")}`);

  const nodes = new Map<string, ExecNode>();
  for (const step of def.steps) {
    nodes.set(step.id, { id: step.id, step, dependencies: [], dependents: [] });
  }

  for (const step of def.steps) {
    const node = nodes.get(step.id)!;
    for (const depId of step.dependsOn ?? []) {
      const depNode = nodes.get(depId)!;
      node.dependencies.push(depNode);
      depNode.dependents.push(node);
    }
  }

  const entryNodes = [...nodes.values()].filter(n => n.dependencies.length === 0);
  const exitNodes = [...nodes.values()].filter(n => n.dependents.length === 0);

  return { name: def.name, version: def.version ?? "1.0.0", entryNodes, exitNodes, nodes };
}

/** Convenience: parse + validate + build graph */
export function compile(raw: string, format?: "json" | "yaml"): ExecutionGraph {
  return buildExecutionGraph(parseDefinition(raw, format));
}