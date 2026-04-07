"""
CVD SOP Web API Server
Flask 后端，封装 tools/ 目录下的查询工具
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import subprocess
import os

app = Flask(__name__)
CORS(app)

# tools/ 目录放在 server.py 同级
TOOLS_DIR = os.path.join(os.path.dirname(__file__), "tools")

def run_tool(script: str, question: str) -> str:
    """执行 Python 工具脚本，返回结果"""
    try:
        result = subprocess.run(
            ["python3", script, question],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=TOOLS_DIR
        )
        return result.stdout if result.returncode == 0 else ""
    except Exception as e:
        return ""

@app.route("/api/query", methods=["POST"])
def query():
    data = request.get_json()
    question = data.get("question", "").strip()

    if not question:
        return jsonify({"error": "问题不能为空"}), 400

    # 判断路由（由后端 Agent 决定）
    q_lower = question.lower()
    is_neo = any(k in q_lower for k in ["原因", "根因", "导致", "造成", "涉及", "哪些异常"])
    is_rag = any(k in q_lower for k in ["流程", "处理", "规范", "怎么", "sop", "当机"])

    neo4j_output = ""
    rag_output = ""

    if is_neo or (is_neo == is_rag):
        neo4j_output = run_tool("neo4j_query_tool.py", question)

    if is_rag or (not is_neo and not is_rag):
        rag_output = run_tool("rag_query_tool.py", question)

    sources = []
    if neo4j_output and "未找到" not in neo4j_output:
        sources.append("Neo4j 图谱")
    if rag_output and "未找到" not in rag_output:
        sources.append("SOP 文档")

    combined = ""
    if neo4j_output and "未找到" not in neo4j_output:
        combined += f"【Neo4j 图谱查询结果】\n{neo4j_output}\n\n"
    if rag_output and "未找到" not in rag_output:
        combined += f"【SOP 文档查询结果】\n{rag_output}"

    if not combined.strip():
        combined = "抱歉，数据库中没有找到相关信息。"

    answer = polish_with_llm(question, combined, sources)

    return jsonify({
        "answer": answer,
        "sources": sources
    })

def polish_with_llm(question: str, data: str, sources: list) -> str:
    """LLM 润色回答"""
    import requests

    source_str = " + ".join(sources) if sources else "未知来源"
    prompt = f"""用户问题：{question}
数据来源：{source_str}

原始数据：
{data}

请将以上数据整理成简洁、专业的回答，使用中文，直接回复用户问题，不要提及你是AI或数据处理过程。"""

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "qwen3:4b",
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {"temperature": 0.3, "num_predict": 500}
            },
            timeout=90
        )
        if response.status_code == 200:
            return response.json().get("response", data)[:2000]
    except Exception:
        pass

    return data[:2000] if len(data) > 2000 else data

if __name__ == "__main__":
    print("启动 CVD SOP API Server...")
    app.run(host="0.0.0.0", port=18765, debug=False)
