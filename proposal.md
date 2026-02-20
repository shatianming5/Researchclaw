# Proposal：Researchclaw（OpenClaw）本地部署与全量自检

## 背景

目标是在一台 Linux 主机上把 `Researchclaw` 从源码完整跑起来，并把关键路径（构建、Gateway 启动、基础 RPC、自检用例）全部跑通；同时给出一份可复用的配置方案，用于接入 BigModel/GLM（参考官方 FAQ：`https://docs.bigmodel.cn/cn/coding-plan/faq`）。

## 范围（Scope）

- ✅ 从源码安装依赖、构建产物（CLI + Control UI）
- ✅ 本地启动 Gateway（loopback），并进行健康检查
- ✅ 运行仓库完整测试套件（`pnpm test`）
- ✅ 给出 BigModel/GLM 接入的推荐配置（不写入任何真实 key）
- ⛔ 不包含：WhatsApp/Telegram/Slack/Discord 等真实通道的生产级联调（需要你提供对应平台账号/Token/回调域名/设备扫码等）

## 先决条件（Prerequisites）

- Node.js `>= 22`（本机已满足）
- `pnpm`（建议用 `corepack` 管理）
- 可选：Docker（仅当你要跑 Docker e2e 测试或走容器化部署）

## BigModel/GLM 接入方案（两种可选）

### A) 用内置 `zai` Provider（OpenClaw 原生 GLM 接入）

适合你已经有 **Z.AI/GLM** 的 key（环境变量通常为 `ZAI_API_KEY`）。

配置示例（`~/.openclaw/openclaw.json`，JSON5 同理）：

```json
{
  "env": { "ZAI_API_KEY": "${ZAI_API_KEY}" },
  "agents": {
    "defaults": {
      "model": { "primary": "zai/glm-4.7" },
      "models": { "zai/glm-4.7": { "alias": "GLM" } }
    }
  }
}
```

### B) 用 BigModel FAQ 的 `open.bigmodel.cn` 端点（自定义 Provider）

适合你手里的 key 是 BigModel（open.bigmodel.cn）体系，并希望严格按 FAQ 的 baseUrl 来走（例如 OpenAI 兼容 `chat/completions`）。

FAQ 中常见 baseUrl（用于你核对）：

- 通用 OpenAI 兼容（含 `chat/completions`）：`https://open.bigmodel.cn/api/paas/v4`
- Coding 专用：`https://open.bigmodel.cn/api/coding/paas/v4`
- Claude Code / Anthropic 兼容：`https://open.bigmodel.cn/api/anthropic`

OpenClaw 自定义 Provider（示例：把 BigModel 当 OpenAI 兼容来用）：

```json
{
  "env": { "BIGMODEL_API_KEY": "${BIGMODEL_API_KEY}" },
  "agents": {
    "defaults": {
      "model": { "primary": "bigmodel/glm-4.7" },
      "models": { "bigmodel/glm-4.7": { "alias": "GLM (BigModel)" } }
    }
  },
  "models": {
    "mode": "merge",
    "providers": {
      "bigmodel": {
        "baseUrl": "https://open.bigmodel.cn/api/paas/v4",
        "apiKey": "${BIGMODEL_API_KEY}",
        "api": "openai-completions",
        "models": [
          {
            "id": "glm-4.7",
            "name": "GLM 4.7",
            "reasoning": false,
            "input": ["text"],
            "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
            "contextWindow": 128000,
            "maxTokens": 8192
          }
        ]
      }
    }
  }
}
```

说明：

- `reasoning/contextWindow/maxTokens/cost` 建议你按 BigModel 最新文档与实际计费/限额修正（示例里用保守占位值）。
- 任何 key 都只通过环境变量注入，不要提交到仓库。

## 部署步骤（本地 From Source）

```bash
cd Researchclaw

# 1) 安装 pnpm（推荐）
corepack enable
corepack prepare pnpm@10.23.0 --activate

# 2) 安装依赖
pnpm install

# 3) 构建 dist/
pnpm build

# 4) 构建 Control UI（build 会清理 dist，建议在 build 之后跑）
pnpm ui:build
```

## 运行与验收（Acceptance）

### 1) 启动 Gateway（建议先跳过通道）

```bash
OPENCLAW_SKIP_CHANNELS=1 CLAWDBOT_SKIP_CHANNELS=1 node openclaw.mjs gateway --port 18789 --verbose --force
```

### 2) 健康检查

```bash
node openclaw.mjs gateway call health --json
```

### 3) 全量测试

```bash
pnpm test
```

验收标准：

- `pnpm test` 退出码为 0
- `gateway call health` 返回 `ok: true`
- Gateway 能在 `ws://127.0.0.1:18789` 正常监听（loopback）
