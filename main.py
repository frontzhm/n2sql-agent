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


async def sse_stream():
    """模拟 NLP-to-SQL 全流程进度推送的 SSE 生成器"""
    # 按自然语言转 SQL 的真实处理流程编排步骤顺序：
    # 抽取关键词 → 召回字段/指标/值 → 合并召回信息 → 过滤指标/表 → 添加上下文 → 生成 SQL → 验证 SQL → 执行 SQL
    texts = [
        {"type": "progress", "step": "抽取关键词", "status": "running"},
        {"type": "progress", "step": "抽取关键词", "status": "success"},
        {"type": "progress", "step": "召回字段", "status": "running"},
        {"type": "progress", "step": "召回指标", "status": "running"},
        {"type": "progress", "step": "召回值", "status": "running"},
        {"type": "progress", "step": "召回值", "status": "success"},
        {"type": "progress", "step": "召回字段", "status": "success"},
        {"type": "progress", "step": "召回指标", "status": "success"},
        {"type": "progress", "step": "合并召回信息", "status": "running"},
        {"type": "progress", "step": "合并召回信息", "status": "success"},
        {"type": "progress", "step": "过滤指标", "status": "running"},
        {"type": "progress", "step": "过滤表", "status": "running"},
        {"type": "progress", "step": "过滤指标", "status": "success"},
        {"type": "progress", "step": "过滤表", "status": "success"},
        {"type": "progress", "step": "添加额外上下文", "status": "running"},
        {"type": "progress", "step": "添加额外上下文", "status": "success"},
        {"type": "progress", "step": "生成 SQL", "status": "running"},
        {"type": "progress", "step": "生成 SQL", "status": "success"},
        {"type": "progress", "step": "验证 SQL", "status": "running"},
        {"type": "progress", "step": "验证 SQL", "status": "success"},
        {"type": "progress", "step": "执行 SQL", "status": "running"},
        {
            "type": "result",
            "data": [
                {"gender": "男", "sales_amount": 135370.5},
                {"gender": "女", "sales_amount": 143789.0},
            ],
        },
        {"type": "progress", "step": "执行 SQL", "status": "success"},
    ]
    # 逐条推送 SSE 事件，每条间隔 200ms 模拟异步处理延迟
    for text in texts:
        yield f"data: {json.dumps(text, ensure_ascii=False)}\n\n"
        await asyncio.sleep(0.2)


@app.post("/api/query")
async def query():
    """自然语言查询入口，以 SSE 流式返回处理进度和最终结果"""
    return StreamingResponse(
        sse_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
