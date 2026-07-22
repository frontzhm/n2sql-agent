import asyncio
from langgraph.graph import START, END, StateGraph
from typing import Dict, Any, TypedDict


# State 定义图中各节点间流转的共享状态
# total=False 表示所有字段均为可选，避免每次返回都必须包含全部字段
class State(TypedDict, total=False):
    error: Any  # SQL 校验错误信息，None 表示校验通过


# 运行时上下文，如数据库连接、LLM 客户端等长生命周期对象
RuntimeContext = Dict[str, Any]

# ============================
# 节点函数
# ============================

# 1. 从用户自然语言中提取关键词
def extract_keywords(state: State) -> State:
    return state

# 2. 并行检索：召回相关字段（列名）
def recall_column(state: State) -> State:
    return state

# 3. 并行检索：召回相关指标
def recall_metric(state: State) -> State:
    return state

# 4. 并行检索：召回相关枚举值
def recall_value(state: State) -> State:
    return state

# 5. 汇总并行检索结果
def merge_retrieved_info(state: State) -> State:
    return state

# 6. 并行过滤：选出候选表
def filter_table(state: State) -> State:
    return state

# 7. 并行过滤：筛选相关指标
def filter_metric(state: State) -> State:
    return state

# 8. 补充额外上下文（如表结构、业务规则）
def add_extra_context(state: State) -> State:
    return state

# 9. 基于上下文 + 用户问题生成 SQL
def generate_sql(state: State) -> State:
    return state

# 10. 校验 SQL 的语法与语义正确性
def validate_sql(state: State) -> State:
    return state

# 11. 校验失败时对 SQL 进行修正
def correct_sql(state: State) -> State:
    return state

# 12. 执行 SQL 并返回结果
def run_sql(state: State) -> State:
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

# 输出 Mermaid 流程图（可在 mermaid.live 中可视化）
mind = graph.get_graph().draw_mermaid()
print(mind)