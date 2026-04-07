"""
LLM 合并润色工具
"""
import requests

OLLAMA_URL = "http://localhost:11434"
LLM_MODEL = "qwen3:4b"

def polish(question: str, neo4j_result: str, rag_result: str, route: str) -> str:
    neo4j_short = neo4j_result[:2000] if len(neo4j_result) > 2000 else neo4j_result
    rag_short = rag_result[:2500] if len(rag_result) > 2500 else rag_result

    prompt = f"""你是一个半导体设备异常处理助手。根据以下信息回答用户问题。

用户问题：{question}
数据来源：{"Neo4j图谱 + SOP文档" if route == "both" else ("Neo4j图谱" if route == "neo4j" else "SOP文档")}

【Neo4j 图谱数据】
注意：格式"【X】的原因 ← Y"表示X异常的原因是由Y引起的（X是结果，Y是原因）。
{neo4j_short}

【SOP 文档内容】
{rag_short}

要求：
- 用自然语言回答，不要暴露原始数据
- 标注来源：图谱 / SOP文档
- 信息不足时说"暂无相关信息"
- 回答专业、准确、易读"""

    response = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={
            "model": LLM_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.3, "num_predict": 600},
            "think": False
        },
        timeout=90
    )
    return response.json()["response"].strip()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python3 polish_tool.py <question> <neo4j_result> <rag_result> <route>")
        sys.exit(1)
    question = sys.argv[1]
    neo4j_result = sys.argv[2].replace("\\n", "\n") if len(sys.argv) > 2 else ""
    rag_result = sys.argv[3].replace("\\n", "\n") if len(sys.argv) > 3 else ""
    route = sys.argv[4] if len(sys.argv) > 4 else "both"
    print(polish(question, neo4j_result, rag_result, route))
