import { describe, expect, it } from "vitest";
import "./test-helpers/fast-coding-tools.js";
import { createOpenClawCodingTools } from "./pi-tools.js";

describe("experiment tool gating", () => {
  it("removes experiment for unauthorized senders", () => {
    const tools = createOpenClawCodingTools({ senderIsOwner: false });
    const toolNames = tools.map((tool) => tool.name);
    expect(toolNames).not.toContain("experiment");
  });

  it("keeps experiment for authorized senders", () => {
    const tools = createOpenClawCodingTools({ senderIsOwner: true });
    const toolNames = tools.map((tool) => tool.name);
    expect(toolNames).toContain("experiment");
  });

  it("defaults to removing experiment when owner status is unknown", () => {
    const tools = createOpenClawCodingTools();
    const toolNames = tools.map((tool) => tool.name);
    expect(toolNames).not.toContain("experiment");
  });
});
