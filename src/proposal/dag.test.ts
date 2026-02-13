import { describe, expect, it } from "vitest";
import type { PlanDag } from "./schema.js";
import { validateDag } from "./dag.js";

describe("proposal/dag", () => {
  it("topologically sorts an acyclic dag", () => {
    const dag: PlanDag = {
      nodes: [
        { id: "a", type: "noop", tool: "manual", inputs: [], outputs: [] },
        { id: "b", type: "noop", tool: "manual", inputs: [], outputs: [] },
      ],
      edges: [{ from: "a", to: "b" }],
    };
    const res = validateDag(dag);
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.order).toEqual(["a", "b"]);
    }
  });

  it("detects cycles", () => {
    const dag: PlanDag = {
      nodes: [
        { id: "a", type: "noop", tool: "manual", inputs: [], outputs: [] },
        { id: "b", type: "noop", tool: "manual", inputs: [], outputs: [] },
      ],
      edges: [
        { from: "a", to: "b" },
        { from: "b", to: "a" },
      ],
    };
    const res = validateDag(dag);
    expect(res.ok).toBe(false);
  });
});
