#!/usr/bin/env python3
"""LLM 抽取 - streaming 调 Ollama，分批处理"""
import sys, re, json, time
import requests
from neo4j import GraphDatabase

OLLAMA = 'http://localhost:11434'
MODEL = 'qwen3:4b'
NEO4J_URI = 'bolt://localhost:17687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'password'

N_BATCHES = 8  # 每份文档分8批，每批约144行

def read_lines(path):
    with open(path, encoding='utf-8') as f:
        return [l.strip() for l in f if l.strip()]

def split_batches(lines, n):
    size = (len(lines) + n - 1) // n
    return [lines[i:i+size] for i in range(0, len(lines), size)]

PROMPT = """你是半导体CVD/PVD知识抽取专家。从以下文档提取实体关系JSON数组（最多8条）。

关系：causes(导致), leads_to(引发), is_type(是类型), related_to(相关), requires(需要执行), occurs_in(发生在), produces(产生)

实体类型：abnormal_type, defect_code, equipment, cause, action, inspection

规则：subject和object必须是具体实体名词（不是动词短语），置信度0.7~1.0。

只返回JSON数组：[{"subject":"A","subject_type":"类型","predicate":"causes","object":"B","object_type":"类型","confidence":0.9}]

文档：
{text}

只返回JSON数组。"""

def extract(text):
    """用 streaming 调 Ollama"""
    payload = {
        'model': MODEL,
        'messages': [{'role': 'user', 'content': PROMPT.format(text=text)}],
        'stream': True
    }
    
    full = ''
    try:
        with requests.post(f'{OLLAMA}/api/chat', json=payload, stream=True, timeout=600) as r:
            for line in r.iter_lines():
                if line:
                    try:
                        d = json.loads(line)
                        if 'message' in d and 'content' in d['message']:
                            full += d['message']['content']
                    except:
                        pass
    except Exception as e:
        print(f"    streaming错误: {e}")
        return []
    
    # 解析JSON
    m = re.search(r'\[.*\]', full, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except:
            print(f"    JSON解析失败: {full[:100]}")
    else:
        print(f"    无JSON: {full[:100]}")
    return []

def write(items, source):
    if not items:
        return 0
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    count = 0
    with driver.session() as s:
        for item in items:
            sn = item.get('subject', '').strip()[:80]
            on = item.get('object', '').strip()[:80]
            pred = item.get('predicate', 'RELATED').strip().upper()
            sc = item.get('subject_type', 'unknown')
            oc = item.get('object_type', 'unknown')
            conf = float(item.get('confidence', 0.8))
            if len(sn) < 2 or len(on) < 2:
                continue
            s.run("MERGE (e:LLM_Entity {name: $n, category: $c})", {'n': sn, 'c': sc})
            s.run("MERGE (e:LLM_Entity {name: $n, category: $c})", {'n': on, 'c': oc})
            s.run("""
                MATCH (a:LLM_Entity {name: $sn}), (b:LLM_Entity {name: $on})
                MERGE (a)-[r:EXTRACTED]->(b)
                SET r.predicate=$pred, r.confidence=$conf, r.source=$src
            """, {'sn': sn, 'on': on, 'pred': pred, 'conf': conf, 'src': source})
            count += 1
    driver.close()
    return count

def main():
    print("读取文件...")
    film = read_lines('/Users/momobalap/.openclaw/workspace/cvd-kg/Array1_薄膜异常_text.txt')
    cvd = read_lines('/Users/momobalap/.openclaw/workspace/cvd-kg/Array1_CVD_text_cn.txt')
    print(f"薄膜: {len(film)} 行, CVD: {len(cvd)} 行")
    
    film_batches = split_batches(film, N_BATCHES)
    cvd_batches = split_batches(cvd, N_BATCHES)
    
    total = 0
    
    # 处理薄膜
    for i, batch in enumerate(film_batches):
        text = '\n'.join(batch)
        tag = f'薄膜SOP_{i}'
        print(f"\n[{i+1}/{N_BATCHES*2}] {tag} ({len(batch)}行)...")
        sys.stdout.flush()
        items = extract(text[:2500])
        n = len(items)
        w = write(items, tag)
        total += w
        print(f"  → {n}条抽取, {w}条写入")
        if i < len(film_batches)-1:
            time.sleep(2)  # 避免连续调用
    
    # 处理CVD
    for i, batch in enumerate(cvd_batches):
        text = '\n'.join(batch)
        tag = f'CVD_SOP_{i}'
        print(f"\n[{N_BATCHES+i+1}/{N_BATCHES*2}] {tag} ({len(batch)}行)...")
        sys.stdout.flush()
        items = extract(text[:2500])
        n = len(items)
        w = write(items, tag)
        total += w
        print(f"  → {n}条抽取, {w}条写入")
        if i < len(cvd_batches)-1:
            time.sleep(2)
    
    print(f"\n=== 完成! 共写入 {total} 条关系 ===")
    
    # 验证
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as s:
        r = s.run("MATCH (n:LLM_Entity) RETURN count(n) as c").single()
        print(f"LLM_Entity节点: {r[0] if r else 0}")
        r = s.run("MATCH ()-[r]->() WHERE r.predicate IS NOT NULL RETURN count(r) as c").single()
        print(f"EXTRACTED关系: {r[0] if r else 0}")
        
        r = s.run("""
            MATCH (a:LLM_Entity)-[r]->(b:LLM_Entity)
            RETURN a.name as A, r.predicate as P, b.name as B
            ORDER BY r.confidence DESC
            LIMIT 10
        """)
        print("\nTop关系:")
        for rec in r:
            print(f"  {rec['A']} --[{rec['P']}]--> {rec['B']}")
    driver.close()

if __name__ == '__main__':
    main()
