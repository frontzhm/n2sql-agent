import asyncio
import json
from langgraph.graph import START, END, StateGraph
from langgraph.runtime import Runtime
from typing import Annotated, Dict, Any, TypedDict


# 节点函数 → 用户可见的中文步骤名映射
STEP_NAMES: dict[str, str] = {
    "extract_keywords": "抽取关键词",
    "recall_column": "召回字段",
    "recall_metric": "召回指标",
    "recall_value": "召回值",
    "merge_retrieved_info": "合并召回信息",
    "filter_table": "过滤表",
    "filter_metric": "过滤指标",
    "add_extra_context": "添加额外上下文",
    "generate_sql": "生成 SQL",
    "validate_sql": "验证 SQL",
    "correct_sql": "修正 SQL",
    "run_sql": "执行 SQL",
}


# 推送进度的辅助函数
def push_progress(runtime: Runtime, step_name: str, status: str) -> None:
    runtime.stream_writer({"type": "progress", "step": step_name, "status": status})


# State 定义图中各节点间流转的共享状态
# total=False 表示所有字段均为可选，避免每次返回都必须包含全部字段
# 并行分支需要 reducer：单值用 last_value 覆盖
class State(TypedDict, total=False):
    error: Annotated[Any, lambda a, b: b]  # 冲突时保留最后一个值（last-value-wins）


# 运行时上下文，如数据库连接、LLM 客户端等长生命周期对象
class RuntimeContext(TypedDict, total=False):
    pass

# ============================
# 节点函数
# ============================

# 1. 从用户自然语言中提取关键词
async def extract_keywords(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["extract_keywords"], "running")
    await asyncio.sleep(1)
    # TODO: 调用 LLM 提取关键词
    push_progress(runtime, STEP_NAMES["extract_keywords"], "success")
    return state

# 2. 并行检索：召回相关字段（列名）
async def recall_column(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["recall_column"], "running")
    await asyncio.sleep(1)
    # TODO: 向量检索召回相关列
    push_progress(runtime, STEP_NAMES["recall_column"], "success")
    return state

# 3. 并行检索：召回相关指标
async def recall_metric(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["recall_metric"], "running")
    await asyncio.sleep(1)
    # TODO: 向量检索召回相关指标
    push_progress(runtime, STEP_NAMES["recall_metric"], "success")
    return state

# 4. 并行检索：召回相关枚举值
async def recall_value(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["recall_value"], "running")
    await asyncio.sleep(1)
    # TODO: 向量检索召回相关值
    push_progress(runtime, STEP_NAMES["recall_value"], "success")
    return state

# 5. 汇总并行检索结果
async def merge_retrieved_info(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["merge_retrieved_info"], "running")
    await asyncio.sleep(1)
    # TODO: 去重、排序、合并
    push_progress(runtime, STEP_NAMES["merge_retrieved_info"], "success")
    return state

# 6. 并行过滤：选出候选表
async def filter_table(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["filter_table"], "running")
    await asyncio.sleep(1)
    # TODO: LLM 选出最相关的表
    push_progress(runtime, STEP_NAMES["filter_table"], "success")
    return state

# 7. 并行过滤：筛选相关指标
async def filter_metric(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["filter_metric"], "running")
    await asyncio.sleep(1)
    # TODO: LLM 筛选相关指标
    push_progress(runtime, STEP_NAMES["filter_metric"], "success")
    return state

# 8. 补充额外上下文（如表结构、业务规则）
async def add_extra_context(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["add_extra_context"], "running")
    await asyncio.sleep(1)
    # TODO: 补充表结构、业务规则等上下文
    push_progress(runtime, STEP_NAMES["add_extra_context"], "success")
    return state

# 9. 基于上下文 + 用户问题生成 SQL
async def generate_sql(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["generate_sql"], "running")
    await asyncio.sleep(1)
    # TODO: LLM 生成 SQL
    push_progress(runtime, STEP_NAMES["generate_sql"], "success")
    return state

# 10. 校验 SQL 的语法与语义正确性
async def validate_sql(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["validate_sql"], "running")
    await asyncio.sleep(1)
    # TODO: SQL 语法校验
    push_progress(runtime, STEP_NAMES["validate_sql"], "success")
    return state

# 11. 校验失败时对 SQL 进行修正
async def correct_sql(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["correct_sql"], "running")
    await asyncio.sleep(1)
    # TODO: LLM 修正 SQL
    push_progress(runtime, STEP_NAMES["correct_sql"], "success")
    return state

# 12. 执行 SQL 并返回结果
async def run_sql(state: State, runtime: Runtime) -> State:
    push_progress(runtime, STEP_NAMES["run_sql"], "running")
    await asyncio.sleep(1)
    # TODO: 连接数据库执行 SQL，返回结果
    runtime.stream_writer({
        "type": "result",
        "data": [
            {"gender": "男", "sales_amount": 135370.5},
            {"gender": "女", "sales_amount": 143789.0},
        ],
    })
    push_progress(runtime, STEP_NAMES["run_sql"], "success")
    return state

# ============================
# DAG 图构建
# ============================

graph = (
    StateGraph(state_schema=State, context_schema=RuntimeContext)
    # 注册所有节点
    .add_node(extract_keywords)
    .add_node(recall_column)
    .add_node(recall_metric)
    .add_node(recall_value)
    .add_node(merge_retrieved_info)
    .add_node(filter_table)
    .add_node(filter_metric)
    .add_node(add_extra_context)
    .add_node(generate_sql)
    .add_node(validate_sql)
    .add_node(correct_sql)
    .add_node(run_sql)
    # START → 提取关键词
    .add_edge(START, "extract_keywords")
    # 关键词并行分发到三条检索通道
    .add_edge("extract_keywords", "recall_column")
    .add_edge("extract_keywords", "recall_metric")
    .add_edge("extract_keywords", "recall_value")
    # 三条检索通道汇聚
    .add_edge("recall_column", "merge_retrieved_info")
    .add_edge("recall_metric", "merge_retrieved_info")
    .add_edge("recall_value", "merge_retrieved_info")
    # 汇总后并行分发到表过滤和指标过滤
    .add_edge("merge_retrieved_info", "filter_table")
    .add_edge("merge_retrieved_info", "filter_metric")
    # 过滤结果汇聚
    .add_edge("filter_table", "add_extra_context")
    .add_edge("filter_metric", "add_extra_context")
    # 补充上下文 → 生成 SQL
    .add_edge("add_extra_context", "generate_sql")
    # 生成 SQL → 校验
    .add_edge("generate_sql", "validate_sql")
    # 条件分支：校验通过 → 执行 SQL，否则 → 修正后执行
    .add_conditional_edges(
        source="validate_sql",
        path=lambda state: "run_sql" if state["error"] is None else "correct_sql",
        path_map={"run_sql": "run_sql", "correct_sql": "correct_sql"},
    )
    .add_edge("correct_sql", "run_sql")
    .add_edge("run_sql", END)
    .compile()
)

# ============================
# 演示：执行图并观察节点流转
# ============================
if __name__ == "__main__":
    # 1. 输出 Mermaid 流程图
    print("=" * 50)
    print("Mermaid 流程图：")
    print("=" * 50)
    mermaid = graph.get_graph().draw_mermaid()
    print(mermaid)

    # 2. 以初始状态运行图（无 error，走 validate_sql → run_sql 路径）
    print("=" * 50)
    print("执行图（正常路径）：")
    print("=" * 50)
    initial_state: State = {"error": None}

    async def run_demo():
        async for event in graph.astream(initial_state, stream_mode="custom"):
            print(json.dumps(event, ensure_ascii=False))

    asyncio.run(run_demo())