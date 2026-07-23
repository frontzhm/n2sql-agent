from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# 创建 FastAPI 应用实例
app = FastAPI(title="n2sql-agent")

# 配置 CORS 中间件，允许跨域请求（开发阶段全放通）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- 基础接口 ----------

@app.get("/hello")
async def hello():
    """健康检查 / 测试接口"""
    return {"msg": "Hello FastAPI + uv"}


# ---------- SSE 流式接口 ----------

from fastapi.responses import StreamingResponse
import json
import asyncio


async def sse_stream(query_text: str):
    """调用 LangGraph DAG，通过 stream_mode='custom' 接收节点推送的进度，以 SSE 流式输出"""
    from app.agent.graph import graph, State

    initial_state: State = {"query": query_text, "error": None}
    # stream_mode="custom" 只接收节点内通过 runtime.stream_writer 推送的消息
    async for event in graph.astream(initial_state, stream_mode="custom"):
        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


@app.post("/api/query")
async def query(payload: dict):
    """自然语言查询入口，以 SSE 流式返回处理进度和最终结果"""
    query_text = payload.get("query", "")
    return StreamingResponse(
        sse_stream(query_text),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
