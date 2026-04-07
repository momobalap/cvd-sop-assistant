"""
Neo4j 图谱查询工具
"""
import os
import re
from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:17687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

CAUSED_BY_PREDS = {"CAUSED_BY", "TRIGGERED_BY", "LEADS_FROM"}

def format_rel(row) -> str:
    s, p, o = row["subject"], row["pred"], row["object"]
    if p in CAUSED_BY_PREDS:
        return f"  【{s}】的原因 ← {o}"
    return f"  {s} --({p})--> {o}"

def extract_keywords(text: str) -> list:
    english = re.findall(r'[A-Za-z0-9_\-]+', text)
    stop = set('的的么呢了是啊在和有没就也还但而及或与等及其更最又再能可会只能则必须应该应当异常问题')
    chinese = [c for c in text if '\u4e00' <= c <= '\u9fff' and c not in stop]
    return english + chinese

def query(question: str) -> str:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    kw = extract_keywords(question)
    results_text = ""

    def run_and_match(cypher: str) -> list:
        rows = driver.session().run(cypher).data()
        if kw:
            return [r for r in rows if any(k in (r.get("subject") or "") or k in (r.get("object") or "") for k in kw)]
        return rows

    with driver.session() as s:
        if any(k in question for k in ["原因", "导致", "造成", "引起", "起因"]):
            rows = run_and_match("""
                MATCH (a)-[r:EXTRACTED]->(b)
                WHERE toLower(r.predicate) CONTAINS 'caus'
                   OR toLower(r.predicate) CONTAINS 'lead'
                   OR toLower(r.predicate) CONTAINS 'trigger'
                   OR toLower(r.predicate) CONTAINS 'reason'
                RETURN a.name AS subject, r.predicate AS pred, b.name AS object, r.source AS source
            """)
            if rows:
                results_text += "【Neo4j - 异常原因关联】\n"
                seen = set()
                for row in rows[:50]:
                    key = (row["subject"], row["pred"], row["object"])
                    if key not in seen:
                        seen.add(key)
                        results_text += format_rel(row) + "\n"
                if len(rows) > 50:
                    results_text += f"  （...共 {len(rows)} 条）\n"

        if any(k in question for k in ["步骤", "处理", "流程", "怎么"]):
            rows = run_and_match("""
                MATCH (a)-[r:EXTRACTED]->(b)
                WHERE toLower(r.predicate) CONTAINS 'step'
                   OR toLower(r.predicate) CONTAINS 'action'
                   OR toLower(r.predicate) CONTAINS 'flow'
                   OR toLower(r.predicate) CONTAINS 'process'
                RETURN a.name AS subject, r.predicate AS pred, b.name AS object, r.source AS source
            """)
            if rows:
                results_text += "\n【Neo4j - 处理步骤关联】\n"
                seen = set()
                for row in rows[:50]:
                    key = (row["subject"], row["pred"], row["object"])
                    if key not in seen:
                        seen.add(key)
                        results_text += format_rel(row) + "\n"

        if any(k in question for k in ["涉及", "相关", "哪些", "包括", "关联"]):
            rows = run_and_match("""
                MATCH (a)-[r:EXTRACTED]->(b)
                RETURN a.name AS subject, r.predicate AS pred, b.name AS object, r.source AS source
            """)
            if rows:
                results_text += "\n【Neo4j - 相关实体】\n"
                seen = set()
                for row in rows[:50]:
                    key = (row["subject"], row["pred"], row["object"])
                    if key not in seen:
                        seen.add(key)
                        results_text += format_rel(row) + "\n"

        if not results_text:
            rows = run_and_match("""
                MATCH (a)-[r:EXTRACTED]->(b)
                RETURN a.name AS subject, r.predicate AS pred, b.name AS object, r.source AS source
            """)
            if rows:
                results_text = "【Neo4j - 相关实体】\n"
                seen = set()
                for row in rows[:50]:
                    key = (row["subject"], row["pred"], row["object"])
                    if key not in seen:
                        seen.add(key)
                        results_text += format_rel(row) + "\n"

    driver.close()
    return results_text if results_text else "【Neo4j】未找到相关结构化数据"

if __name__ == "__main__":
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else "RF_VDC异常的原因"
    print(query(q))
