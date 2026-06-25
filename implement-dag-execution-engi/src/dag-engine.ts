===
enum NodeState {
  Pending = "pending",
  Running = "running",
  Succeeded = "succeeded",
  Failed = "failed",
  Skipped = "skipped",
}

type NodeId = string;

interface DAGNode {
  id: NodeId;
  execute: (ctx: ExecutionContext) => Promise<unknown>;
}

interface DAGEdge {
  from: NodeId;
  to: NodeId;
  condition?: (ctx: ExecutionContext) => boolean;
}

interface ExecutionContext {
  results: Map<NodeId, unknown>;
  getState: (id: NodeId) => NodeState | undefined;
  skip: (id: NodeId) => void;
  fail: (id: NodeId, error: Error) => void;
}

type LifecycleEvent =
  | { type: "node:started"; nodeId: NodeId }
  | { type: "node:succeeded"; nodeId: NodeId; result: unknown }
  | { type: "node:failed"; nodeId: NodeId; error: Error }
  | { type: "node:skipped"; nodeId: NodeId }
  | { type: "workflow:completed" }
  | { type: "workflow:failed"; error: Error };

type EventListener = (event: LifecycleEvent) => void;

class DAGValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DAGValidationError";
  }
}

class DAGEngine {
  private nodes: Map<NodeId, DAGNode> = new Map();
  private edges: DAGEdge[] = [];
  private dependents: Map<NodeId, Set<NodeId>> = new Map();
  private dependencies: Map<NodeId, Set<Node<EdgeId>>> = new Map();
  private states: Map<NodeId, NodeState> = new Map();
  private listeners: EventListener[] = [];
  private settled: PromiseWithResolvers<void> | null = null;
  private pendingCount = 0;

  addNode(node: DAGNode): this {
    if (this.nodes.has(node.id)) {
      throw new DAGValidationError(`Duplicate node id: ${node.id}`);
    }
    this.nodes.set(node.id, node);
    this.dependents.set(node.id, new Set());
    this.dependencies.set(node.id, new Set());
    return this;
  }

  addEdge(edge: DAGEdge): this {
    if (!this.nodes.has(edge.from) || !this.nodes.has(edge.to)) {
      throw new DAGValidationError(
        `Edge references unknown node(s): ${edge.from} -> ${edge.to}`
      );
    }
    this.edges.push(edge);
    this.dependents.get(edge.from)!.add(edge.to);
    this.dependencies.get(edge.to)!.add(edge.from);
    return this;
  }

  onEvent(listener: EventListener): this {
    this.listeners.push(listener);
    return this;
  }

  private emit(event: LifecycleEvent): void {
    for (const listener of this.listeners) {
      try { listener(event); } catch { /* swallow listener errors */ }
    }
  }

  validate(): void {
    for (const edge of this.edges) {
      const visited = new Set<NodeId>();
      const hasCycle = (current: NodeId): boolean => {
        if (visited.has(current)) return true;
        visited.add(current);
        for (const dep of this.dependents.get(current) ?? []) {
          if (dep === edge.to || hasCycle(dep)) return true;
        }
        visited.delete(current);
        return false;
      };
      if (hasCycle(edge.from)) {
        throw new DAGValidationError(`Cycle detected involving edge ${edge.from} -> ${edge.to}`);
      }
    }
  }

  async run(): Promise<Map<NodeId, NodeState>> {
    if (this.nodes.size === 0) return this.states;
    this.validate();
    this.states.clear();
    for (const id of this.nodes.keys()) {
      this.states.set(id, NodeState.Pending);
    }
    this.settled = Promise.withResolvers<void>();
    this.pendingCount = 0;

    const roots = [...this.nodes.keys()].filter(
      (id) => this.dependencies.get(id)!.size === 0
    );

    if (roots.length === 0 && this.nodes.size > 0) {
      throw new DAGValidationError("No root nodes found; all nodes have dependencies");
    }

    for (const root of roots) {
      this.tryRunNode(root);
    }

    await this.settled.promise;
    return new Map(this.states);
  }

  private tryRunNode(id: NodeId): void {
    if (this.states.get(id) !== NodeState.Pending) return;

    const deps = this.dependencies.get(id)!;
    for (const dep of deps) {
      const depState = this.states.get(dep);
      if (depState === NodeState.Failed || depState === NodeState.Skipped) {
        this.states.set(id, NodeState.Skipped);
        this.emit({ type: "node:skipped", nodeId: id });
        this.propagate(id);
        return;
      }
      if (depState !== NodeState.Succeeded) return;
    }

    const edgeConditions = this.edges.filter((e) => e.to === id && e.condition);
    for (const edge of edgeConditions) {
      const ctx = this.makeContext();
      try {
        if (!edge.condition!(ctx)) {
          this.states.set(id, NodeState.Skipped);
          this.emit({ type: "node:skipped", nodeId: id });
          this.propagate(id);
          return;
        }
      } catch {
        this.states.set(id, NodeState.Skipped);
        this.emit({ type: "node:skipped", nodeId: id });
        this.propagate(id);
        return;
      }
    }

    this.executeNode(id);
  }

  private async executeNode(id: NodeId): Promise<void> {
    this.states.set(id, NodeState.Running);
    this.emit({ type: "node:started", nodeId: id });
    this.pendingCount++;

    const node = this.nodes.get(id)!;
    const ctx = this.makeContext();
    try {
      const result = await node.execute(ctx);
      this.states.set(id, NodeState.Succeeded);
      this.emit({ type: "node:succeeded", nodeId: id, result });
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.states.set(id, NodeState.Failed);
      this.emit({ type: "node:failed", nodeId: id, error });
    }

    this.pendingCount--;
    this.propagate(id);
    this.checkSettled();
  }

  private propagate(completedId: NodeId): void {
    const deps = this.dependents.get(completedId) ?? new Set();
    for (const depId of deps) {
      this.tryRunNode(depId);
    }
  }

  private checkSettled(): void {
    const allDone = [...this.states.values()].every(
      (s) => s !== NodeState.Pending && s !== NodeState.Running
    );
    if (allDone && this.pendingCount === 0) {
      const hasFailure = [...this.states.values()].some((s) => s === NodeState.Failed);
      if (hasFailure) {
        this.emit({ type: "workflow:failed", error: new Error("One or more nodes failed") });
      } else {
        this.emit({ type: "workflow:completed" });
      }
      this.settled?.resolve();
    }
  }

  private makeContext(): ExecutionContext {
    const engine = this;
    return {
      results: new Map(
        [...engine.states.entries()]
          .filter(([, s]) => s === NodeState.Succeeded)
          .map(([id]) => [id, undefined])
      ),
      getState: (id) => engine.states.get(id),
      skip(id: NodeId) {
        if (engine.states.get(id) === NodeState.Pending) {
          engine.states.set(id, NodeState.Skipped);
          engine.emit({ type: "node:skipped", nodeId: id });
          engine.propagate(id);
          engine.checkSettled();
        }
      },
      fail(id: NodeId, error: Error) {
        if (engine.states.get(id) !== NodeState.Succeeded) {
          engine.states.set(id, NodeState.Failed);
          engine.emit({ type: "node:failed", nodeId: id, error });
          engine.propagate(id);
          engine.checkSettled();
        }
      },
    };
  }
}

export { DAGEngine, DAGNode, DAGEdge, DAGValidationError, NodeState, ExecutionContext, LifecycleEvent };