===
import { describe, it, expect } from "vitest";
import { parseDefinition, validate, buildExecutionGraph, compile } from "./workflow-parser";

const validWorkflow = {
  name: "test-pipeline",
  version: "1.0.0",
  steps: [
    { id: "start", type: "task", handler: "init" },
    { id: "branch", type: "branch", dependsOn: ["start"], condition: { expr: "x>0", then: "a", else: "b" } },
    { id: "a", type: "task", dependsOn: ["branch"], handler: "doA" },
    { id: "b", type: "task", dependsOn: ["branch"], handler: "doB" },
    { id: "end", type: "join", dependsOn: ["a", "b"] },
  ],
};

describe("parseDefinition", () => {
  it("parses JSON", () => {
    const def = parseDefinition(JSON.stringify(validWorkflow));
    expect(def.name).toBe("test-pipeline");
    expect(def.steps).toHaveLength(5);
  });

  it("parses YAML", () => {
    const yaml = `name: yaml-test\nsteps:\n  - id: s1\n    type: task`;
    const def = parseDefinition(yaml, "yaml");
    expect(def.name).toBe("yaml-test");
    expect(def.steps[0].id).toBe("s1");
  });

  it("auto-detects JSON format", () => {
    const def = parseDefinition(JSON.stringify(validWorkflow));
    expect(def.steps.length).toBeGreaterThan(0);
  });
});

describe("validate", () => {
  it("passes a valid DAG", () => {
    expect(validate(validWorkflow as any)).toHaveLength(0);
  });

  it("detects duplicate ids", () => {
    const w = { name: "x", steps: [{ id: "a", type: "task" }, { id: "a", type: "task" }] };
    const errs = validate(w as any);
    expect(errs.some(e => e.code === "duplicate_id")).toBe(true);
  });

  it("detects invalid dependsOn references", () => {
    const w = { name: "x", steps: [{ id: "a", type: "task", dependsOn: ["missing"] }] };
    const errs = validate(w as any);
    expect(errs.some(e => e.code === "invalid_ref")).toBe(true);
  });

  it("detects cycles", () => {
    const w = { name: "x", steps: [
      { id: "a", type: "task", dependsOn: ["b"] },
      { id: "b", type: "task", dependsOn: ["a"] },
    ]};
    const errs = validate(w as any);
    expect(errs.some(e => e.code === "cycle")).toBe(true);
  });

  it("detects self-cycle", () => {
    const w = { name: "x", steps: [{ id: "a", type: "task", dependsOn: ["a"] }] };
    const errs = validate(w as any);
    expect(errs.some(e => e.code === "cycle")).toBe(true);
  });

  it("detects missing name", () => {
    const w = { steps: [{ id: "a", type: "task" }] };
    const errs = validate(w as any);
    expect(errs.some(e => e.code === "missing_field" && !e.stepId)).toBe(true);
  });

  it("detects invalid type", () => {
    const w = { name: "x", steps: [{ id: "a", type: "bogus" }] };
    const errs = validate(w as any);
    expect(errs.some(e => e.code === "type_mismatch")).toBe(true);
  });

  it("detects delegate without agentRef", () => {
    const w = { name: "x", steps: [{ id: "a", type: "delegate" }] };
    const errs = validate(w as any);
    expect(errs.some(e => e.code === "missing_field" && e.stepId === "a")).toBe(true);
  });

  it("detects no-entry-point when all steps depend on something", () => {
    const w = { name: "x", steps: [
      { id: "a", type: "task", dependsOn: ["b"] },
      { id: "b", type: "task", dependsOn: ["a"] },
    ]};
    const errs = validate(w as any);
    expect(errs.some(e => e.code === "no_entry")).toBe(true);
  });
});

describe("buildExecutionGraph", () => {
  it("builds a correct graph from valid workflow", () => {
    const graph = buildExecutionGraph(validWorkflow as any);
    expect(graph.entryNodes).toHaveLength(1);
    expect(graph.entryNodes[0].id).toBe("start");
    expect(graph.exitNodes).toHaveLength(1);
    expect(graph.exitNodes[0].id).toBe("end");
    expect(graph.nodes.get("end")!.dependencies).toHaveLength(2);
    expect(graph.nodes.get("start")!.dependents).toHaveLength(1);
  });

  it("throws on invalid workflow", () => {
    const w = { name: "x", steps: [{ id: "a", type: "task", dependsOn: ["z"] }] };
    expect(() => buildExecutionGraph(w as any)).toThrow(/Validation failed/);
  });

  it("handles linear chain", () => {
    const w = { name: "chain", steps: [
      { id: "a", type: "task" },
      { id: "b", type: "task", dependsOn: ["a"] },
      { id: "c", type: "task", dependsOn: ["b"] },
    ]};
    const graph = buildExecutionGraph(w as any);
    expect(graph.entryNodes[0].id).toBe("a");
    expect(graph.exitNodes[0].id).toBe("c");
  });
});

describe("compile", () => {
  it("end-to-end parses and builds graph", () => {
    const graph = compile(JSON.stringify(validWorkflow));
    expect(graph.name).toBe("test-pipeline");
    expect(graph.nodes.size).toBe(5);
  });
});