#!/usr/bin/env python3
"""导入薄膜异常处理 SOP 到 Neo4j"""

from neo4j import GraphDatabase

NEO4J_URI = 'bolt://localhost:17687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'password'

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def run_cypher(query, params=None):
    with driver.session() as session:
        result = session.run(query, params or {})
        try:
            return [dict(r) for r in result]
        except:
            return result.consume()

def import_schema():
    """从 cypher 文件导入 schema"""
    print("导入薄膜异常 SOP Schema...")
    
    with open('/Users/momobalap/.openclaw/workspace/cvd-kg/13-create-film-anomaly-schema.cypher', 'r') as f:
        content = f.read()
    
    # 移除注释和 RETURN 语句
    import re
    content = re.sub(r'//.*', '', content)
    
    # 在 CREATE 之前加分号
    content = re.sub(r'\nCREATE', ';\nCREATE', content)
    
    statements = [s.strip() for s in content.split(';') if s.strip() and 'CREATE' in s]
    
    print(f"找到 {len(statements)} 个 CREATE 语句")
    
    for i, stmt in enumerate(statements):
        try:
            run_cypher(stmt)
        except Exception as e:
            print(f"  警告 [{i}]: {str(e)[:60]}")
        if (i+1) % 20 == 0:
            print(f"  进度: {i+1}/{len(statements)}")
    
    print("Schema 导入完成")

def verify():
    print("\n验证导入...")
    
    queries = [
        ("Document", "MATCH (d:Document) RETURN count(d) as c"),
        ("Section", "MATCH (s:Section) RETURN count(s) as c"),
        ("AbnormalType", "MATCH (a:AbnormalType) RETURN count(a) as c"),
        ("DefectCode", "MATCH (d:DefectCode) RETURN count(d) as c"),
        ("RootCause", "MATCH (r:RootCause) RETURN count(r) as c"),
        ("InspectionItem", "MATCH (i:InspectionItem) RETURN count(i) as c"),
        ("AlarmCode", "MATCH (a:AlarmCode) RETURN count(a) as c"),
        ("Equipment", "MATCH (e:Equipment) RETURN count(e) as c"),
        ("Action", "MATCH (a:Action) RETURN count(a) as c"),
        ("Role", "MATCH (r:Role) RETURN count(r) as c"),
        ("HAS_CAUSE", "MATCH ()-[r:HAS_CAUSE]->() RETURN count(r) as c"),
        ("REQUIRES_INSPECTION", "MATCH ()-[r:REQUIRES_INSPECTION]->() RETURN count(r) as c"),
    ]
    
    for name, query in queries:
        result = run_cypher(query)
        if result:
            count = result[0]['c'] if isinstance(result[0], dict) else 0
            print(f"  {name}: {count}")

def query_examples():
    """示例查询"""
    print("\n=== 示例查询 ===\n")
    
    print("1. CVD Mura 的成因:")
    result = run_cypher('''
        MATCH (a:AbnormalType {id: "CVD_Mura"})-[:HAS_CAUSE]->(r:RootCause)
        RETURN r.cause as 成因
    ''')
    for r in result:
        print(f"   - {r['成因']}")
    
    print("\n2. Diffuser 相关的 Particle 成因:")
    result = run_cypher('''
        MATCH (rc:RootCause)-[:RELATED_TO]->(e:Equipment {id: "Diffuser"})
        MATCH (a:AbnormalType)-[:HAS_CAUSE]->(rc)
        RETURN a.name as 异常类型, rc.cause as 成因
    ''')
    for r in result:
        print(f"   {r['异常类型']}: {r['成因']}")
    
    print("\n3. Arcing 需要哪些加检项目:")
    result = run_cypher('''
        MATCH (a:AbnormalType {id: "Arcing"})-[:REQUIRES_INSPECTION]->(i:InspectionItem)
        RETURN i.name as 加检项目, i.description as 说明
    ''')
    for r in result:
        print(f"   {r['加检项目']}: {r['说明']}")
    
    print("\n4. RF_VDC Alarm 会导致哪些异常:")
    result = run_cypher('''
        MATCH (ac:AlarmCode {id: "RF_VDC"})-[:CAUSES]->(a:AbnormalType)
        RETURN a.name as 异常类型
    ''')
    for r in result:
        print(f"   {r['异常类型']}")

if __name__ == '__main__':
    import_schema()
    verify()
    query_examples()
    driver.close()
