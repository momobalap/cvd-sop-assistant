#!/usr/bin/env python3
"""
LLM 自动抽取文档关系 → Neo4j 图谱
每次处理一个 chunk，让 LLM 提取实体和关系
"""

import requests
import json
import re
import chromadb
from neo4j import GraphDatabase
from typing import List, Dict

OLLAMA = 'http://localhost:11434'
LLM_MODEL = 'qwen3:4b'
EMBED_MODEL = 'herald/dmeta-embedding-zh'
EMBED_DIM = 768

NEO4J_URI = 'bolt://localhost:17687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'password'

def emb(text):
    r = requests.post(f'{OLLAMA}/api/embed', json={'model': EMBED_MODEL, 'input': text}, timeout=30)
    return r.json()['embeddings'][0]

def emb_batch(texts):
    r = requests.post(f'{OLLAMA}/api/embed', json={'model': EMBED_MODEL, 'input': texts}, timeout=120)
    return r.json()['embeddings']

def chat(prompt, timeout=120):
    r = requests.post(f'{OLLAMA}/api/chat', json={
        'model': LLM_MODEL,
        'messages': [{'role': 'user', 'content': prompt}],
        'stream': False
    }, timeout=timeout)
    return r.json()['message']['content']

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def run_cypher(q, p=None):
    with driver.session() as s:
        return s.run(q, p or {}).consume()

def clear_and_prepare():
    """清空旧的抽取数据（保留 schema 骨架）"""
    print("清空旧抽取数据...")
    run_cypher("MATCH (n:Extracted) DETACH DELETE n")
    run_cypher("MATCH ()-[r:EXTRACTED_FROM]->() DELETE r")
    print("  已清空 Extracted 节点和关系")

def extract_from_chunk(chunk_text: str, chunk_id: str, section: str) -> List[Dict]:
    """用 LLM 从 chunk 中抽取关系"""
    
    prompt = f"""你是半导体 CVD/PVD 领域的知识抽取专家。从以下技术文档中，用结构化 JSON 抽取实体和关系。

要求：
- 只抽取有明确因果或关联的关系
- 一个 chunk 可能包含 0~5 个关系，不要强行抽取
- JSON 格式如下（数组）：
[
  {{
    "subject": "实体A",
    "subject_type": "异常类型|缺陷代码|设备部件|气体|工序",
    "predicate": "关系类型",
    "object": "实体B", 
    "object_type": "异常类型|缺陷代码|设备部件|气体|工序|处理动作",
    "confidence": 0.0~1.0,
    "raw_text": "原文摘要"
  }}
]
如果没有任何关系，返回空数组 []

关系类型(predicate)包括：
- causes: A 导致 B（异常→缺陷、原因→结果）
- requires: A 需要/执行 B（异常→处理动作）
- occurs_in: A 发生在 B（异常→机台）
- related_to: A 与 B 相关（设备部件→异常）
- produces: A 产生 B（设备/操作→缺陷）

技术术语：
- CVD, PVD, T/C, P/C, ACLS, DSSL, ATM Robot, WVD
- FC01~FC15 (CVD缺陷代码), S004, S05G, S05S (PVD缺陷代码)
- Diffuser, Susceptor, Shadow Frame, Scrubber, MFC, RF Generator
- Arcing, Mura, Particle, 刮伤, 破片, Splash
- Chamber Clean, Cycle Purge, PM, Dummy片确认

文档内容：
---
{chunk_text[:1500]}
---

只返回 JSON，不要其他文字。"""

    try:
        response = chat(prompt, timeout=60)
        
        # 提取 JSON
        json_match = re.search(r'\[.*\]', response, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            for item in data:
                item['chunk_id'] = chunk_id
                item['section'] = section
                item['source_text'] = chunk_text[:200]
            return data
    except Exception as e:
        pass
    
    return []

def store_extracted(extractions: List[Dict]):
    """存储抽取结果到 Neo4j"""
    
    for ex in extractions:
        subject = ex['subject'].strip()
        obj = ex['object'].strip()
        pred = ex['predicate'].strip()
        conf = float(ex.get('confidence', 0.8))
        raw = ex.get('raw_text', '')[:200]
        chunk_id = ex['chunk_id']
        section = ex['section']
        
        # 创建 Extracted 节点存储抽取的实体
        run_cypher("""
            MERGE (e:Extracted {name: $name, category: $cat})
        """, {'name': subject, 'cat': ex['subject_type']})
        
        run_cypher("""
            MERGE (e:Extracted {name: $name, category: $cat})
        """, {'name': obj, 'cat': ex['object_type']})
        
        # 创建关系
        rel_type = pred.upper()
        
        run_cypher(f"""
            MATCH (s:Extracted {{name: $subject, category: $scat}})
            MATCH (o:Extracted {{name: $obj, category: $ocat}})
            MERGE (s)-[r:{rel_type}]->(o)
            SET r.confidence = $conf,
                r.raw_text = $raw,
                r.section = $section,
                r.chunk_id = $chunk_id
        """, {
            'subject': subject, 'scat': ex['subject_type'],
            'obj': obj, 'ocat': ex['object_type'],
            'conf': conf, 'raw': raw,
            'section': section, 'chunk_id': chunk_id
        })

def process_collection(chroma_path: str, collection_name: str, doc_name: str, max_chunks: int = 50):
    """处理一个文档的 chunks"""
    
    print(f"\n处理文档: {doc_name}")
    print(f"  Chroma: {chroma_path}")
    print(f"  Collection: {collection_name}")
    
    client = chromadb.PersistentClient(path=chroma_path)
    collection = client.get_or_create_collection(collection_name)
    
    # 取所有 chunks
    result = collection.get(include=['documents', 'metadatas'])
    
    total = min(len(result['ids']), max_chunks)
    print(f"  总 chunks: {len(result['ids'])}, 本次处理: {total}")
    
    extracted_total = 0
    
    for i in range(total):
        chunk_id = result['ids'][i]
        text = result['documents'][i]
        section = result['metadatas'][i].get('section', '')
        
        if i % 10 == 0:
            print(f"  进度: {i}/{total}")
        
        # 抽取
        extractions = extract_from_chunk(text, chunk_id, section)
        
        if extractions:
            store_extracted(extractions)
            extracted_total += len(extractions)
            print(f"    chunk_{i}: 找到 {len(extractions)} 条关系")
    
    print(f"  完成! 共抽取 {extracted_total} 条关系")
    return extracted_total

def verify_and_query():
    """验证抽取结果 + 多跳推理测试"""
    print("\n" + "=" * 60)
    print("验证抽取结果 + 多跳推理测试")
    print("=" * 60)
    
    # 统计
    result = run_cypher("""
        MATCH (n:Extracted) 
        RETURN n.category as category, count(*) as count
        ORDER BY count DESC
    """)
    print("\n抽取实体统计:")
    for r in result:
        print(f"  {r['category']}: {r['count']}")
    
    result = run_cypher("""
        MATCH ()-[r]->() 
        WHERE r.confidence > 0
        RETURN type(r) as rel_type, count(*) as count
        ORDER BY count DESC
    """)
    print("\n关系类型统计:")
    for r in result:
        print(f"  {r['rel_type']}: {r['count']}")
    
    # 多跳推理测试
    print("\n" + "=" * 60)
    print("多跳推理测试")
    print("=" * 60)
    
    queries = [
        ("Diffuser 洁净度 → 导致 → 什么异常？", """
            MATCH (s:Extracted {name: $name})-[:CAUSES|RELATED_TO]->(o)
            RETURN s.name as 起点, type(relationships((s)-[:CAUSES|RELATED_TO]->(o))) as 关系, o.name as 终点
        """),
        ("Diffuser 相关的一切", """
            MATCH (s:Extracted)-[r]->(o)
            WHERE s.name CONTAINS 'Diffuser' OR o.name CONTAINS 'Diffuser'
            RETURN s.name as A, type(r) as 关系, o.name as B
            LIMIT 20
        """),
        ("FC07 或 Particle 聚集的成因", """
            MATCH (s:Extracted)-[r]->(o)
            WHERE o.name CONTAINS 'FC07' OR o.name CONTAINS 'Particle'
               OR s.name CONTAINS 'FC07' OR s.name CONTAINS 'Particle'
            RETURN s.name as 原因, type(r) as 关系, o.name as 结果
            LIMIT 20
        """),
        ("Arcing 的所有成因", """
            MATCH (s:Extracted)-[r]->(o)
            WHERE o.name CONTAINS 'Arcing' OR o.name CONTAINS '电弧'
            RETURN s.name as 原因, type(r) as 关系
            LIMIT 20
        """),
    ]
    
    for desc, query in queries:
        print(f"\n【{desc}】")
        result = run_cypher(query, {'name': 'Diffuser'})
        if result:
            for r in result[:10]:
                print(f"  {dict(r)}")
        else:
            print("  (无结果)")

def full_rag_with_kg(question: str):
    """完整 RAG + 图谱融合查询"""
    print(f"\n{'='*60}")
    print(f"完整查询: {question}")
    print(f"{'='*60}")
    
    # 1. 图谱查询
    print("\n【Neo4j 图谱结果】")
    
    queries = [
        ("实体匹配", f"""
            MATCH (n:Extracted)
            WHERE n.name =~ $p OR n.name CONTAINS $kw
            RETURN n.name as name, n.category as category, 'exact' as match
            LIMIT 10
        """),
        ("一跳关系", f"""
            MATCH (s:Extracted)-[r]->(o)
            WHERE s.name =~ $p OR o.name =~ $p
            RETURN s.name as A, type(r) as 关系, o.name as B, r.confidence as 置信度
            LIMIT 15
        """),
    ]
    
    all_kg_context = []
    for qname, q in queries:
        for kw in question.split():
            result = run_cypher(q, {'p': f'(?i).*{kw}.*', 'kw': kw})
            for r in result:
                entry = f"  {dict(r)}"
                if entry not in all_kg_context:
                    all_kg_context.append(entry)
    
    if all_kg_context:
        for c in all_kg_context[:20]:
            print(c)
    else:
        print("  (无)")
    
    # 2. RAG 查询
    print("\n【RAG 检索结果】")
    import importlib
    film_mod = importlib.import_module('15-film-anomaly-rag')
    cvd_mod = importlib.import_module('12-cvd-qdrant')
    film_search = film_mod.search
    cvd_search = cvd_mod.search
    
    film = film_search(question, 5)
    cvd = cvd_search(question, 3)
    
    ctx = "【RAG 检索】\n"
    for r in film[:3]:
        ctx += f"• {r['heading']}\n  {r['text'][:300]}\n\n"
    for r in cvd[:2]:
        ctx += f"• {r['heading']}\n  {r['text'][:300]}\n\n"
    
    ctx += "\n【Neo4j 图谱】\n"
    ctx += '\n'.join(all_kg_context[:20])
    
    # 3. LLM 生成
    print("\n【LLM 生成回答】")
    prompt = f"""你是半导体 CVD/PVD 领域专家。

用户问题: "{question}"

以下是检索到的资料：

{ctx}

请用流畅中文回答，结构清晰，因果分明。如果图谱和RAG都有信息，优先用RAG的详细内容。"""

    answer = chat(prompt, timeout=120)
    print(answer)

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("用法:")
        print("  python 16-auto-extract-kg.py extract     抽取并写入 Neo4j")
        print("  python 16-auto-extract-kg.py verify    验证+多跳推理测试")
        print("  python 16-auto-extract-kg.py query <问题>  完整 RAG+图谱 融合查询")
        sys.exit(0)
    
    cmd = sys.argv[1]
    
    if cmd == 'extract':
        clear_and_prepare()
        
        # 处理两个文档
        process_collection(
            '/Users/momobalap/.openclaw/workspace/cvd-kg/chroma_film_anomaly',
            'film_anomaly',
            '薄膜产品异常处理作业指导书',
            max_chunks=80
        )
        
        process_collection(
            '/Users/momobalap/.openclaw/workspace/cvd-kg/chroma_ollama',
            'cvd_sop',
            'CVD设备管控作业指导书',
            max_chunks=50
        )
        
        verify_and_query()
    
    elif cmd == 'verify':
        verify_and_query()
    
    elif cmd == 'query':
        if len(sys.argv) < 3:
            print("用法: query <问题>")
            sys.exit(1)
        full_rag_with_kg(sys.argv[2])
    
    driver.close()
