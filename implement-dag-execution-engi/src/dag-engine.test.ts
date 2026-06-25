===
import { describe, it, expect, vi } from "vitest";
import { DAGEngine, NodeState } from "./dag-engine";

describe("DAGEngine", () => {
  it("executes a linear chain A -> B -> C", async () => {
    const order: string[] = [];
    const engine = new DAGEngine();
    engine
      .addNode({ id: "A", execute: async () => { order.push("A"); return 1; } })
      .addNode({ id: "B", execute: async () => { order.push("B"); return 2; } })
      .addNode({ id: "C", execute: async () => { order.push("C"); return 3; } })
      .addEdge({ from: "A", to: "B" })
      .addEdge({ from: "B", to: "C" });

    const states = await engine.run();
    expect(order).toEqual(["A", "B", "C"]);
    expect(states.get("A")).toBe(NodeState.Succeeded);
    expect(states.get("C")).toBe(NodeState.Succeeded);
  });

  it("executes parallel fan-out and fan-in", async () => {
    const order: string[] = [];
    const engine = new DAGEngine();
    engine
      .addNode({ id: "start", execute: async () => { order.push("start"); } })
      .addNode({ id: "left", execute: async () => { order.push("left"); } })
      .addNode({ id: "right", execute: async () => { order.push("right"); } })
      .addNode({ id: "join", execute: async () => { order.push("join"); } })
      .addEdge({ from: "start", to: "left" })
      .addEdge({ from: "start", to: "right" })
      .addEdge({ from: "left", to: "join" })
      .addEdge({ from: "right", to: "join" });

    await engine.run();
    expect(order[0]).toBe("start");
    expect(order).toContain("left");
    expect(order).toContain("right");
    expect(order[3]).toBe("join");
  });

  it("skips downstream nodes when a dependency fails", async () => {
    const engine = new DAGEngine();
    engine
      .addNode({ id: "A", execute: async () => { throw new Error("boom"); } })
      .addNode({ id: "B", execute: async () => {} })
      .addEdge({ from: "A", to: "B" });

    const states = await engine.run();
    expect(states.get("A")).toBe(NodeState.Failed);
    expect(states.get("B")).toBe(NodeState.Skipped);
  });

  it("applies conditional edges to skip branches", async () => {
    const ran: string[] = [];
    const engine = new DAGEngine();
    engine
      .addNode({ id: "decide", execute: async () => false })
      .addNode({ id: "yes", execute: async () => { ran.push("yes"); } })
      .addNode({ id: "no", execute: async () => { ran.push("no"); } })
      .addEdge({ from: "decide", to: "yes", condition: () => true })
      .addEdge({ from: "decide", to: "no", condition: () => false });

    const states = await engine.run();
    expect(states.get("yes")).toBe(NodeState.Succeeded);
    expect(states.get("no")).toBe(NodeState.Skipped);
    expect(ran).toEqual(["yes"]);
  });

  it("emits lifecycle events in correct order", async () => {
    const events: string[] = [];
    const engine = new DAGEngine();
    engine
      .addNode({ id: "X", execute: async () => 42 })
      .onEvent((e) => events.push(e.type));

    await engine.run();
    expect(events).toEqual(["node:started", "node:succeeded", "workflow:completed"]);
  });

  it("detects cycles on validation", () => {
    const engine = new DAGEngine();
    engine
      .addNode({ id: "A", execute: async () => {} })
      .addNode({ id: "B", execute: async () => {} })
      .addEdge({ from: "A", to: "B" })
      .addEdge({ from: "B", to: "A" });

    expect(() => engine.validate()).toThrow(/Cycle detected/);
  });

  it("allows runtime skip via execution context", async () => {
    const engine = new DAGEngine();
    engine
      .addNode({ id: "A", execute: async (ctx) => { ctx.skip("B"); } })
      .addNode({ id: "B", execute: async () => {} })
      .addEdge({ from: "A", to: "B" });

    const states = await engine.run();
    expect(states.get("B")).toBe(NodeState.Skipped);
  });
});