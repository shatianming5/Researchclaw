import { describe, expect, it, vi } from "vitest";
import { discoverDataset } from "./discovery.js";

describe("proposal/discovery secrets", () => {
  it("adds HF Authorization header when HF_TOKEN is set", async () => {
    const prev = process.env.HF_TOKEN;
    process.env.HF_TOKEN = "hf_test_token";
    try {
      const fetchFn = vi.fn(async (input: string, init?: RequestInit) => {
        const headers = (init?.headers ?? {}) as Record<string, string>;
        expect(headers.authorization).toBe("Bearer hf_test_token");
        if (input.startsWith("https://huggingface.co/api/datasets/squad")) {
          return new Response(JSON.stringify({ id: "squad" }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (input.startsWith("https://datasets-server.huggingface.co/splits?dataset=squad")) {
          return new Response(JSON.stringify({ splits: [{ config: "default", split: "train" }] }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (input.startsWith("https://datasets-server.huggingface.co/rows?dataset=squad")) {
          return new Response(JSON.stringify({ rows: [{ row: { id: 1 } }] }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        return new Response("not found", { status: 404 });
      });

      const res = await discoverDataset({
        input: { name: "squad", platform: "hf" },
        mode: "sample",
        fetchFn,
        timeoutMs: 10_000,
      });

      expect(res.exists).toBe(true);
      expect(fetchFn).toHaveBeenCalled();
    } finally {
      if (prev === undefined) {
        delete process.env.HF_TOKEN;
      } else {
        process.env.HF_TOKEN = prev;
      }
    }
  });

  it("fetches Kaggle metadata when credentials are present", async () => {
    const prevUser = process.env.KAGGLE_USERNAME;
    const prevKey = process.env.KAGGLE_KEY;
    process.env.KAGGLE_USERNAME = "user";
    process.env.KAGGLE_KEY = "key";
    try {
      const fetchFn = vi.fn(async (input: string, init?: RequestInit) => {
        const headers = (init?.headers ?? {}) as Record<string, string>;
        expect(headers.authorization).toMatch(/^Basic\s+/);
        if (input.includes("/api/v1/datasets/view/owner/ds")) {
          return new Response(JSON.stringify({ title: "ds" }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (input.includes("/api/v1/datasets/metadata/owner/ds")) {
          return new Response(JSON.stringify({ files: ["train.csv"] }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        return new Response("not found", { status: 404 });
      });

      const res = await discoverDataset({
        input: { name: "owner/ds", platform: "kaggle" },
        mode: "sample",
        fetchFn,
        timeoutMs: 10_000,
      });

      expect(res.platform).toBe("kaggle");
      expect(res.resolvedId).toBe("owner/ds");
      expect(res.exists).toBe(true);
      expect(res.sample).toBeDefined();
    } finally {
      if (prevUser === undefined) {
        delete process.env.KAGGLE_USERNAME;
      } else {
        process.env.KAGGLE_USERNAME = prevUser;
      }
      if (prevKey === undefined) {
        delete process.env.KAGGLE_KEY;
      } else {
        process.env.KAGGLE_KEY = prevKey;
      }
    }
  });
});
