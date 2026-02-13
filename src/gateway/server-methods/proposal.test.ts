import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ErrorCodes } from "../protocol/index.js";
import { proposalHandlers } from "./proposal.js";

const noop = () => false;

describe("gateway proposal.compile", () => {
  it("compiles a plan package from markdown", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-gateway-proposal-"));
    const workspaceDir = path.join(tmp, "ws");

    const respond = vi.fn();
    await proposalHandlers["proposal.compile"]({
      params: {
        proposalMarkdown: ["# Proposal", "", "Repo: openclaw/openclaw", ""].join("\n"),
        discovery: "off",
        workspaceDir,
        useLlm: false,
      },
      respond,
      context: {} as unknown as Parameters<
        (typeof proposalHandlers)["proposal.compile"]
      >[0]["context"],
      client: null,
      req: { id: "req-1", type: "req", method: "proposal.compile" },
      isWebchatConnect: noop,
    });

    expect(respond).toHaveBeenCalledTimes(1);
    const [ok, payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(true);
    expect(error).toBeUndefined();
    expect(payload).toEqual(
      expect.objectContaining({
        ok: expect.any(Boolean),
        planId: expect.any(String),
        rootDir: expect.any(String),
        report: expect.any(Object),
        paths: expect.any(Object),
      }),
    );

    const rootDir = payload.rootDir;
    expect(rootDir).toContain(path.join(workspaceDir, "experiments", "workdir"));

    await expect(fs.stat(path.join(rootDir, "plan", "plan.dag.json"))).resolves.toBeTruthy();
    await expect(
      fs.stat(path.join(rootDir, "report", "compile_report.json")),
    ).resolves.toBeTruthy();
  });

  it("rejects missing proposalMarkdown", async () => {
    const respond = vi.fn();
    await proposalHandlers["proposal.compile"]({
      params: { discovery: "off", useLlm: false },
      respond,
      context: {} as unknown as Parameters<
        (typeof proposalHandlers)["proposal.compile"]
      >[0]["context"],
      client: null,
      req: { id: "req-2", type: "req", method: "proposal.compile" },
      isWebchatConnect: noop,
    });

    expect(respond).toHaveBeenCalledTimes(1);
    const [ok, _payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(false);
    expect(error).toEqual(
      expect.objectContaining({
        code: ErrorCodes.INVALID_REQUEST,
      }),
    );
  });

  it("rejects invalid discovery mode", async () => {
    const respond = vi.fn();
    await proposalHandlers["proposal.compile"]({
      params: {
        proposalMarkdown: "# Proposal",
        discovery: "nope",
        useLlm: false,
      },
      respond,
      context: {} as unknown as Parameters<
        (typeof proposalHandlers)["proposal.compile"]
      >[0]["context"],
      client: null,
      req: { id: "req-3", type: "req", method: "proposal.compile" },
      isWebchatConnect: noop,
    });

    expect(respond).toHaveBeenCalledTimes(1);
    const [ok, _payload, error] = respond.mock.calls[0] ?? [];
    expect(ok).toBe(false);
    expect(error).toEqual(
      expect.objectContaining({
        code: ErrorCodes.INVALID_REQUEST,
      }),
    );
  });
});
