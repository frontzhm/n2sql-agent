# n2sql-agent

自然语言转 SQL 问答系统，输入一个问题，后端通过 SSE 流式推送 NLP-to-SQL 全流程进度，前端实时展示步骤和结果表格。

## 技术栈

- **后端**：Python 3.12+ / FastAPI / SSE 流式推送
- **前端**：Vue 3 / Vite / pnpm
- **包管理**：后端用 uv，前端用 pnpm

## 快速启动

### 1. 启动后端

```bash
# 安装依赖
uv sync

# 启动（默认 http://localhost:8000）
uv run fastapi dev main.py
```

验证后端是否正常：

```bash
curl http://localhost:8000/hello
# {"msg":"Hello FastAPI + uv"}

curl http://localhost:8000/docs
# 浏览器打开可交互的 Swagger API 文档
```

### 2. 启动前端

```bash
cd frontend
pnpm install
pnpm dev
```

浏览器打开 `http://localhost:5173`，输入自然语言问题即可体验。

> 前端已配置 Vite 代理，`/api` 请求自动转发到后端 `localhost:8000`，无需额外配置。

## 交互方式

### 浏览器

打开 `http://localhost:5173`，输入问题（如"男女性别销售额分别是多少"），页面会实时展示：

1. 处理进度步骤（抽取关键词 → 召回字段 → 生成 SQL → 验证 SQL → 执行 SQL）
2. 最终结果表格

### 命令行（curl）

```bash
# 流式调试（-N 禁用缓冲，看到实时推送）
curl -N -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "男女销售额对比"}'
```

## 项目结构

```
n2sql-agent/
├── main.py              # FastAPI 后端入口
├── pyproject.toml       # Python 项目配置
├── frontend/
│   ├── src/App.vue      # 主页面（消息列表 + SSE 长连接消费）
│   ├── package.json
│   └── vite.config.js   # 含 /api 代理到后端的配置
└── local/               # 本地笔记与学习资料
```
