#!/usr/bin/env python3
"""Import CVD Equipment SOP data into Neo4j using neo4j driver."""

import json
from pathlib import Path
from neo4j import GraphDatabase

# ================== 配置 ==================
NEO4J_URI = "bolt://localhost:17687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# ================== Neo4j 连接 ==================

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def run_cypher(query, params=None):
    """Execute Cypher query."""
    with driver.session() as session:
        result = session.run(query, params or {})
        try:
            return [dict(record) for record in result]
        except:
            return result.consume()

# ================== 导入函数 ==================

def clear_database():
    """Clear all existing data."""
    print("Clearing database...")
    run_cypher("MATCH (n) DETACH DELETE n")
    print("  Database cleared.")

def create_schema():
    """Create the schema from cypher file."""
    print("Creating schema...")
    schema_file = Path(__file__).parent / "06-create-cvd-schema.cypher"
    
    with open(schema_file, 'r') as f:
        content = f.read()
    
    # Remove comments
    import re
    content = re.sub(r'//.*', '', content)
    content = re.sub(r'\n\n+', '\n', content)
    
    # Add semicolons before each CREATE
    content = re.sub(r'\nCREATE', ';\nCREATE', content)
    
    # Split by semicolons and execute each CREATE statement
    statements = [s.strip() for s in content.split(';') if s.strip() and 'CREATE' in s]
    
    print(f"  Found {len(statements)} CREATE statements")
    
    for i, stmt in enumerate(statements):
        try:
            run_cypher(stmt)
        except Exception as e:
            print(f"  Warning at statement {i}: {str(e)[:80]}")
        if (i + 1) % 20 == 0:
            print(f"  Progress: {i + 1}/{len(statements)}")
    
    print("  Schema created.")

def import_images():
    """Import image nodes with section mappings."""
    print("\nImporting images...")
    
    mapping_file = Path(__file__).parent / "image_mapping.json"
    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    base_path = Path(__file__).parent / "images"
    
    for img in data['image_mapping']:
        section_hint = img.get('section_hint', '')
        
        query = """
        CREATE (i:Image {
          id: $id,
          filename: $filename,
          path: $path,
          line_number: $line_number,
          section_hint: $section_hint,
          size_bytes: $size
        })
        """
        
        params = {
            'id': f"img_{img['image_index']:03d}",
            'filename': img['filename'],
            'path': str(base_path / img['filename']),
            'line_number': img['line_number'],
            'section_hint': section_hint,
            'size': img.get('size', 0)
        }
        
        run_cypher(query, params)
    
    print(f"  Created {len(data['image_mapping'])} Image nodes")
    
    # Link images to sections based on section_hint
    print("  Linking images to sections...")
    
    query = """
    MATCH (i:Image), (s:Section)
    WHERE i.section_hint CONTAINS s.number
    CREATE (i)-[:BELONGS_TO]->(s)
    """
    run_cypher(query)
    
    # Also create general BELONGS_TO relation for Document
    query2 = """
    MATCH (i:Image), (d:Document)
    CREATE (i)-[:BELONGS_TO_DOCUMENT]->(d)
    """
    run_cypher(query2)

def verify_import():
    """Verify the import by counting nodes."""
    print("\nVerifying import...")
    
    queries = [
        ("Document", "MATCH (d:Document) RETURN count(d) as count"),
        ("Sections", "MATCH (s:Section) RETURN count(s) as count"),
        ("Equipment", "MATCH (e:Equipment) RETURN count(e) as count"),
        ("Components", "MATCH (c:Component) RETURN count(c) as count"),
        ("Gases", "MATCH (g:Gas) RETURN count(g) as count"),
        ("SOPs", "MATCH (o:SOP) RETURN count(o) as count"),
        ("Roles", "MATCH (r:Role) RETURN count(r) as count"),
        ("Abbrev", "MATCH (a:Abbreviation) RETURN count(a) as count"),
        ("Images", "MATCH (i:Image) RETURN count(i) as count"),
        ("Process", "MATCH (p:Process) RETURN count(p) as count"),
        ("Relationships", "MATCH ()-[r]->() RETURN count(r) as count"),
    ]
    
    for name, query in queries:
        result = run_cypher(query)
        if result:
            try:
                count = result[0]['count'] if isinstance(result[0], dict) else result[0]
                print(f"  {name}: {count}")
            except:
                print(f"  {name}: (error reading)")

def main():
    print("=" * 50)
    print("CVD Equipment SOP - Neo4j Import")
    print("=" * 50)
    
    try:
        clear_database()
        create_schema()
        import_images()
        verify_import()
        
        print("\n" + "=" * 50)
        print("Import complete!")
        print("Access Neo4j at: http://localhost:17474")
        print("=" * 50)
    finally:
        driver.close()

if __name__ == '__main__':
    main()
