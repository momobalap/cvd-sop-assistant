#!/usr/bin/env python3
"""
从两份文档全面构建 Neo4j 知识图谱
- 用规则 + 关键词抽取（不依赖 LLM，避免超时）
- 从文本本身提取结构化关系
"""

import re
import chromadb
from neo4j import GraphDatabase
from typing import List, Dict, Tuple

NEO4J_URI = 'bolt://localhost:17687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'password'

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def run(q, p=None):
    with driver.session() as s:
        result = s.run(q, p or {})
        try:
            return [dict(r) for r in result]
        except:
            return s.consume()

# ========== 关系模式定义 ==========

# CVD Loss Codes
LOSS_CODES = {
    'S001': 'Chamber Unmatch',
    'S002': 'Pump Down Abort',
    'S003': 'Temperature Abort',
    'S004': 'Arcing',
    'S005': 'Manual Abort',
    'S006': 'Edge Mask Error',
    'S05G': 'Gate Splash',
    'S05S': 'S/D Splash',
    'S009': 'Soft Abort',
    'S010': 'Power Abort',
    'S011': 'Pressure Abort',
    'S012': 'Temperature Over Spec',
}

# CVD Film Codes
FILM_CODES = {
    'FC01': '完全没有膜沉积',
    'FC02': '3L破洞',
    'FC03': '不明Particle造成光阻凸起',
    'FC04': 'CVD Splash',
    'FC05': '薄膜太薄',
    'FC06': '薄膜太厚',
    'FC07': 'Particle特殊分布/聚集',
    'FC08': '纤维状Particle',
    'FC09': 'Particle超100颗',
    'FC10': 'Particle尺寸超标',
    'FC11': 'Particle在特定位置',
    'FC12': 'Particle聚集杀伤力大',
    'FC13': 'Particle造成薄膜剥离',
    'FC14': '薄膜颜色异常',
    'FC15': '薄膜不均匀',
}

# Abnormal types
ABNORMAL_TYPES = {
    'CVD_Mura': 'CVD Mura膜厚不均',
    'Arcing': 'Arcing电弧放电',
    'Particle': 'Particle粒子异常',
    '刮伤': '刮伤',
    '破片': '破片缺角',
    'Splash': '金属溅镀异常',
    '边缘未镀膜': '边缘未镀膜区宽度异常',
    '完全无膜': '完全没有膜沉积',
    '3L破洞': '3L破洞',
}

# Equipment
EQUIPMENT = {
    'CVD': '化学气相沉积机',
    'PVD': '物理气相沉积机',
    'P/C': 'Process Chamber',
    'T/C': 'Transfer Chamber',
    'ACLS': '自动 cassette 装载站',
    'ATM Robot': '大气机器人',
    'DSSL': '双单槽 Loadlock',
    'WVD': 'Wafer Vision Detector',
    'MFC': '质量流量控制器',
    'RF Generator': '射频发生器',
    'Scrubber': '气体洗涤器',
    'Diffuser': '气体分布器',
    'Susceptor': '载盘',
    'Shadow Frame': '遮框',
    'Heat Exchanger': '热交换器',
    'Vaccum Robot': '真空机器人',
}

# Root causes
ROOT_CAUSES = {
    'Diffuser洁净度不佳': 'Diffuser孔洞残留颗粒',
    'RPS洁净度不佳': 'RPS Chamber洁净度差',
    'RPS Leak': 'RPS泄漏',
    'J-Tube block': 'J-Tube堵塞',
    'CSD O-ring leak': 'CSD密封圈泄漏',
    '环境Particle': '环境颗粒污染',
    'o-ring屑剥落': '密封件老化碎屑',
    'Glass破片残留': '玻璃碎片残留',
    'Clamp Life过长': 'Clamp寿命过长',
    'Clamp歪斜': 'Clamp位置偏移',
    'Fiber异物': '纤维状异物',
    'Susceptor Arcing': 'Susceptor放电',
    'Self Clean秒数不足': '自清洁时间不够',
    'Precoating film peeling': '预涂层剥离',
}

# Alarm codes
ALARM_CODES = {
    'RF_VDC': 'RF直流电压异常',
    'RF_VPP': 'RF峰值电压异常',
    'RF_RFLCT': 'RF反射功率异常',
    'Generator Problem': '发生器故障',
    'Load Problem': 'Load故障',
    'Hard Arc': '严重拉弧',
    'Micro Arc': '轻微拉弧',
    'Power': '电源异常',
}

# Inspection items
INSPECTION = {
    'TOI': '薄膜外观检测',
    'TMM': '膜厚量测',
    'TEF': '电性测试',
    'TCM': '膜质检测',
    'TST': '张力测试',
    'AOIH': '自动光学检测',
    'STRP': '清洗处理',
    'MACO': '缺陷检测',
}

# SOP actions
SOP_ACTIONS = {
    'Chamber Clean': '腔室清洁',
    'Cycle Purge': '循环吹扫',
    'Dummy片确认': 'Dummy片检验',
    '停机确认': '停机检查',
    '加测电性': '电性加测',
    'STRP清洗': 'STRP清洗',
    'AOI检测': 'AOI光学检测',
    'EMO复位': 'EMO紧急停止复位',
    'Interlock旁路': '联锁旁路操作',
    'MFC吹扫': 'MFC气体吹扫',
    'Leak Test': '泄漏测试',
    'PM保养': '定期保养',
}

# Gases
GASES = {
    'SiH4': '硅烷',
    'NH3': '氨气',
    'PH3': '磷烷',
    'H2': '氢气',
    'N2': '氮气',
    'NF3': '三氟化氮',
    'Ar': '氩气',
    'CDA': '压缩干燥空气',
}

# Process recipes
PROCESSES = {
    'SiN': '氮化硅沉积',
    'a-Si': '非晶硅沉积',
    'N+': 'N+掺杂',
    'Passivation': '钝化处理',
}

# ========== 抽取逻辑 ==========

def extract_causal_patterns(text: str) -> List[Tuple[str, str, str]]:
    """抽取因果关系: 原因 → 导致 → 结果"""
    results = []
    
    patterns = [
        # "A导致B" / "A造成B" / "A引起B"
        (r'([^\s，、。]{2,20}?)导致([^\s，、。]{2,20}?)', 'causes'),
        (r'([^\s，、。]{2,20}?)造成([^\s，、。]{2,20}?)', 'causes'),
        (r'([^\s，、。]{2,20}?)引起([^\s，、。]{2,20}?)', 'causes'),
        (r'([^\s，、。]{2,20}?)使得([^\s，、。]{2,20}?)', 'causes'),
        # "因为A，所以B" / "由于A"
        (r'由于([^\s，、。]{2,20}?)，?([^\s，、。]{2,20}?)异常', 'causes'),
        (r'因为([^\s，、。]{2,20}?)，?([^\s，、。]{2,20}?)发生', 'causes'),
    ]
    
    for pattern, rel_type in patterns:
        matches = re.findall(pattern, text)
        for m in matches:
            if isinstance(m, tuple) and len(m) == 2:
                cause, effect = m[0].strip(), m[1].strip()
                if len(cause) > 1 and len(effect) > 1:
                    results.append((cause, rel_type, effect))
    
    return results

def extract_code_relations(text: str) -> List[Tuple[str, str, str]]:
    """抽取代码关系: Defect Code → 是什么类型"""
    results = []
    
    # CVD Loss Code
    for code, name in LOSS_CODES.items():
        if code in text:
            # 查找后续描述
            idx = text.find(code)
            after = text[idx:idx+100]
            m = re.search(r'[:-]?\s*([^\n。]{3,50})', after)
            desc = m.group(1).strip() if m else name
            results.append((code, 'is_type', desc[:40]))
    
    # CVD Film Code
    for code, name in FILM_CODES.items():
        if code in text:
            results.append((code, 'is_type', name))
    
    return results

def extract_equipment_relations(text: str) -> List[Tuple[str, str, str]]:
    """抽取设备相关关系"""
    results = []
    
    # Equipment - part relations
    equip_patterns = [
        ('CVD', ['Chamber', 'P/C', 'T/C', 'Diffuser', 'Susceptor']),
        ('PVD', ['Chamber', 'Target', 'Shield']),
        ('T/C', ['Robot', 'Slit Valve', 'Loadlock']),
        ('P/C', ['Susceptor', 'Heater', 'MFC', 'RF']),
    ]
    
    for equip, parts in equip_patterns:
        for part in parts:
            if part in text:
                results.append((equip, 'has_part', part))
    
    # Diffuser relations
    if 'Diffuser' in text:
        if 'Particle' in text or '聚集' in text or '残留' in text:
            results.append(('Diffuser', 'causes', 'Particle异常'))
        if '洁净度' in text or '清洁' in text:
            results.append(('Diffuser', 'requires', '清洁保养'))
    
    # Alarm relations
    for alarm in ALARM_CODES:
        if alarm in text:
            if 'Arcing' in text or '电弧' in text:
                results.append((alarm, 'causes', 'Arcing'))
            if 'Mura' in text or '膜厚' in text:
                results.append((alarm, 'causes', 'CVD_Mura'))
    
    return results

def extract_action_relations(text: str) -> List[Tuple[str, str, str]]:
    """抽取处理动作关系"""
    results = []
    
    # SOP patterns
    sop_patterns = [
        (r'([A-Za-z0-9_]+)\s*PM', 'PM保养'),
        (r'Chamber\s*Clean', 'Chamber清洁'),
        (r'Cycle\s*Purge', '循环吹扫'),
        (r'Dummy[片器]?\s*确认', 'Dummy片确认'),
        (r'STRP\s*清洗', 'STRP清洗'),
        (r'AOI\s*检测', 'AOI检测'),
        (r'EMO\s*复位', 'EMO复位'),
        (r'Leak\s*Test', '泄漏测试'),
        (r'MFC\s*吹扫', 'MFC吹扫'),
    ]
    
    for pattern, action in sop_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            # 找相关的异常
            for abnormal in ['Arcing', 'Mura', 'Particle', '刮伤', '破片']:
                if abnormal in text:
                    results.append((action, 'treats', abnormal))
                    break
    
    return results

def extract_all_from_chunk(text: str, chunk_id: str) -> List[Dict]:
    """从 chunk 抽取所有关系"""
    relations = []
    
    # 1. 因果模式
    for cause, rel, effect in extract_causal_patterns(text):
        relations.append({
            'source': cause,
            'target': effect,
            'type': rel.upper(),
            'chunk_id': chunk_id,
            'raw_text': f"{cause} → {effect}"
        })
    
    # 2. Code 类型
    for code, rel, desc in extract_code_relations(text):
        relations.append({
            'source': code,
            'target': desc,
            'type': rel.upper(),
            'chunk_id': chunk_id,
            'raw_text': f"{code} = {desc}"
        })
    
    # 3. 设备关系
    for src, rel, tgt in extract_equipment_relations(text):
        relations.append({
            'source': src,
            'target': tgt,
            'type': rel.upper(),
            'chunk_id': chunk_id,
            'raw_text': f"{src} {rel} {tgt}"
        })
    
    # 4. 处理动作
    for action, rel, abnormal in extract_action_relations(text):
        relations.append({
            'source': action,
            'target': abnormal,
            'type': rel.upper(),
            'chunk_id': chunk_id,
            'raw_text': f"{action} for {abnormal}"
        })
    
    # 去重
    seen = set()
    unique = []
    for r in relations:
        key = (r['source'], r['type'], r['target'])
        if key not in seen and len(r['source']) > 1 and len(r['target']) > 1:
            seen.add(key)
            unique.append(r)
    
    return unique

# ========== Neo4j 写入 ==========

def clear_and_init():
    """清空并初始化"""
    print("清空 Neo4j...")
    run("MATCH (n) DETACH DELETE n")
    
    # 创建索引
    run("CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.name)")
    print("  已清空，创建索引")

def store_relation(rel: Dict):
    """存储单条关系"""
    src = rel['source'][:80]
    tgt = rel['target'][:80]
    rel_type = rel['type'][:30]
    raw = rel.get('raw_text', '')[:150]
    chunk_id = rel.get('chunk_id', '')
    
    run("""
        MERGE (s:Entity {name: $src})
        MERGE (t:Entity {name: $tgt})
        MERGE (s)-[r:RELATES {type: $rel_type, chunk: $chunk_id, raw: $raw}]->(t)
    """, {'src': src, 'tgt': tgt, 'rel_type': rel_type, 'raw': raw, 'chunk_id': chunk_id})

def process_chroma_collection(chroma_path: str, collection_name: str, doc_name: str):
    """处理一个文档"""
    print(f"\n处理: {doc_name}")
    
    client = chromadb.PersistentClient(path=chroma_path)
    collection = client.get_or_create_collection(collection_name)
    result = collection.get(include=['documents', 'metadatas'])
    
    total = len(result['ids'])
    print(f"  总 chunks: {total}")
    
    total_rels = 0
    
    for i in range(total):
        chunk_id = result['ids'][i]
        text = result['documents'][i]
        
        rels = extract_all_from_chunk(text, chunk_id)
        
        for rel in rels:
            store_relation(rel)
            total_rels += 1
        
        if (i + 1) % 20 == 0:
            print(f"  进度: {i+1}/{total}, 累计关系: {total_rels}")
    
    print(f"  完成! 共抽取 {total_rels} 条关系")
    return total_rels

def verify():
    """验证"""
    print("\n" + "="*50)
    print("验证 Neo4j 图谱")
    print("="*50)
    
    # 节点统计
    r = run("MATCH (n:Entity) RETURN count(n) as c")
    print(f"\n节点总数: {r[0]['c']}")
    
    # 关系统计
    r = run("MATCH ()-[r]->() RETURN count(r) as c")
    print(f"关系总数: {r[0]['c']}")
    
    # 关系类型
    r = run("""
        MATCH ()-[r]->() 
        RETURN r.type as type, count(*) as count 
        ORDER BY count DESC LIMIT 20
    """)
    print("\n关系类型分布:")
    for rec in r:
        print(f"  {rec['type']}: {rec['count']}")
    
    # 知名节点的关系
    queries = [
        "Diffuser",
        "Arcing", 
        "Particle",
        "FC07",
        "Chamber Clean",
    ]
    
    print()
    for q in queries:
        r = run("""
            MATCH (s:Entity)-[r]->(t)
            WHERE s.name CONTAINS $q OR t.name CONTAINS $q
            RETURN s.name as A, r.type as 关系, t.name as B
            LIMIT 10
        """, {'q': q})
        if r:
            print(f"\n【{q} 相关关系】")
            for rec in r:
                print(f"  {rec['A']} --[{rec['关系']}]--> {rec['B']}")

if __name__ == '__main__':
    total = 0
    total += process_chroma_collection(
        '/Users/momobalap/.openclaw/workspace/cvd-kg/chroma_film_anomaly',
        'film_anomaly',
        '薄膜产品异常处理作业指导书'
    )
    total += process_chroma_collection(
        '/Users/momobalap/.openclaw/workspace/cvd-kg/chroma_ollama',
        'cvd_sop', 
        'CVD设备管控作业指导书'
    )
    
    verify()
    driver.close()
    print(f"\n全部完成! 共 {total} 条关系")
