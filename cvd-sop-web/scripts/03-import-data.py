#!/usr/bin/env python3
"""
CVD 知识图谱数据导入脚本
从 PostgreSQL 读取数据，导入 Neo4j

使用前先安装依赖:
  pip install psycopg2-binary neo4j tqdm

使用:
  python 03-import-data.py --help
"""

import argparse
import json
import time
from datetime import datetime, timedelta
from tqdm import tqdm

# ================== 配置 ==================

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"  # 修改为你的密码

PG_CONFIG = {
    "host": "your-postgres-host",
    "port": 5432,
    "database": "defect",
    "user": "your-user",
    "password": "your-password"
}

# CVD 设备列表
CVD_EQUIPMENT = [
    "1VDM0120", "1VDM0220", "1VDM0320", "1VDM0420", "1VDM0520",
    "1VDM0620", "1VDM0720", "1VDM0820", "1VDM0920", "1VDM1020",
    "1VDM1120"
]

# ================== Neo4j 连接 ==================

def get_neo4j_driver():
    from neo4j import GraphDatabase
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ================== 节点导入 ==================

def import_employees(driver, cursor):
    """导入员工数据"""
    print("Importing employees...")
    cursor.execute("SELECT emp_no, emp_name, dept_no, dept_name, factory_code FROM hr_emp")
    count = 0
    with driver.session() as session:
        for row in tqdm(cursor, total=cursor.rowcount, desc="Employees"):
            session.run("""
                MERGE (e:Employee {emp_no: $emp_no})
                SET e.empName = $emp_name,
                    e.deptNo = $dept_no,
                    e.deptName = $dept_name,
                    e.factoryCode = $factory_code
            """, emp_no=row[0], emp_name=row[1], dept_no=row[2], dept_name=row[3], factory_code=row[4])
            count += 1
    print(f"  Imported {count} employees")

def import_defect_classifications(driver, cursor):
    """导入缺陷分类"""
    print("Importing defect classifications...")
    cursor.execute("SELECT id, category, name, description, ui_color FROM defect_classifications")
    count = 0
    with driver.session() as session:
        for row in tqdm(cursor, total=cursor.rowcount, desc="Classifications"):
            session.run("""
                MERGE (c:DefectClassification {id: $id})
                SET c.category = $category,
                    c.name = $name,
                    c.description = $description,
                    c.uiColor = $ui_color
            """, id=str(row[0]), category=row[1], name=row[2], description=row[3], ui_color=row[4])
            count += 1
    print(f"  Imported {count} classifications")

def import_defect_events(driver, cursor, days=90):
    """导入缺陷事件"""
    print(f"Importing defect events (last {days} days)...")
    cursor.execute(f"""
        SELECT id, batch_id, eq_name, dept, scope, run_at, interval_hours,
               total_defect_count, total_glass_count, trouble_chambers,
               trouble_chamber_count, total_cluster_count, status, assigned_to,
               factory_code
        FROM defect_event
        WHERE run_at >= NOW() - INTERVAL '{days} days'
    """)
    count = 0
    with driver.session() as session:
        for row in tqdm(cursor, total=cursor.rowcount, desc="Events"):
            session.run("""
                MERGE (e:DefectEvent {eventId: $id})
                SET e.batchId = $batch_id,
                    e.eqName = $eq_name,
                    e.dept = $dept,
                    e.scope = $scope,
                    e.runAt = $run_at,
                    e.intervalHours = $interval_hours,
                    e.totalDefectCount = $total_defect_count,
                    e.totalGlassCount = $total_glass_count,
                    e.troubleChambers = $trouble_chambers,
                    e.troubleChamberCount = $trouble_chamber_count,
                    e.totalClusterCount = $total_cluster_count,
                    e.status = $status,
                    e.assignedTo = $assigned_to,
                    e.factoryCode = $factory_code
            """,
                id=row[0], batch_id=row[1], eq_name=row[2], dept=row[3],
                scope=row[4], run_at=row[5], interval_hours=row[6],
                total_defect_count=row[7], total_glass_count=row[8],
                trouble_chambers=json.dumps(row[9]) if row[9] else None,
                trouble_chamber_count=row[10], total_cluster_count=row[11],
                status=row[12], assigned_to=row[13], factory_code=row[14]
            )
            count += 1
    print(f"  Imported {count} events")

def import_defect_clusters(driver, cursor, days=90):
    """导入腔室聚集"""
    print(f"Importing defect clusters (last {days} days)...")
    cursor.execute(f"""
        SELECT id, event_id, chamber, glass_count, total_defect_count,
               avg_dd, max_defect_count, priority_score, anomaly_status,
               anomaly_type, auto_analysis_summary, factory_code
        FROM chamber_cluster
        WHERE created_at >= NOW() - INTERVAL '{days} days'
    """)
    count = 0
    with driver.session() as session:
        for row in tqdm(cursor, total=cursor.rowcount, desc="Clusters"):
            session.run("""
                MERGE (c:DefectCluster {clusterId: $id})
                SET c.eventId = $event_id,
                    c.chamber = $chamber,
                    c.glassCount = $glass_count,
                    c.totalDefectCount = $total_defect_count,
                    c.avgDd = $avg_dd,
                    c.maxDefectCount = $max_defect_count,
                    c.priorityScore = $priority_score,
                    c.anomalyStatus = $anomaly_status,
                    c.anomalyType = $anomaly_type,
                    c.autoAnalysisSummary = $auto_analysis_summary,
                    c.factoryCode = $factory_code
            """,
                id=row[0], event_id=row[1], chamber=row[2], glass_count=row[3],
                total_defect_count=row[4], avg_dd=row[5], max_defect_count=row[6],
                priority_score=row[7], anomaly_status=row[8], anomaly_type=row[9],
                auto_analysis_summary=row[10], factory_code=row[11]
            )
            count += 1
    print(f"  Imported {count} clusters")
    return count

def import_defect_points(driver, cursor, days=90, batch_size=1000):
    """导入缺陷记录（分批导入）"""
    print(f"Importing defect points (last {days} days)...")

    eqp_list = "', '".join(CVD_EQUIPMENT)

    cursor.execute(f"""
        SELECT COUNT(*) FROM defect_point
        WHERE defect_time >= NOW() - INTERVAL '{days} days'
          AND eqp_id IN ('{eqp_list}')
    """)
    total = cursor.fetchone()[0]
    print(f"  Total records to import: {total}")

    cursor.execute(f"""
        SELECT defect_id, event_id, glass_id, lot_id, product_id,
               eqp_id, chamber, ope_id, defect_code, defect_size,
               x_coord, y_coord, countnum, cc, defect_time, factory_code
        FROM defect_point
        WHERE defect_time >= NOW() - INTERVAL '{days} days'
          AND eqp_id IN ('{eqp_list}')
        ORDER BY defect_time DESC
    """)

    batch = []
    count = 0
    with driver.session() as session:
        with tqdm(total=total, desc="DefectPoints") as pbar:
            for row in cursor:
                batch.append({
                    "defect_id": row[0],
                    "event_id": row[1],
                    "glass_id": row[2],
                    "lot_id": row[3],
                    "product_id": row[4],
                    "eqp_id": row[5],
                    "chamber": row[6],
                    "station_id": row[7],  # ope_id 是站点
                    "defect_code": row[8],
                    "defect_size": row[9],
                    "x_coord": float(row[10]) if row[10] else None,
                    "y_coord": float(row[11]) if row[11] else None,
                    "countnum": row[12],
                    "cc": row[13],
                    "defect_time": row[14],
                    "factory_code": row[15]
                })

                if len(batch) >= batch_size:
                    # 批量写入
                    session.run("""
                        UNWIND $batch AS row
                        MERGE (d:DefectRecord {defectId: row.defect_id})
                        SET d.eventId = row.event_id,
                            d.glassId = row.glass_id,
                            d.lotId = row.lot_id,
                            d.productId = row.product_id,
                            d.eqpId = row.eqp_id,
                            d.chamber = row.chamber,
                            d.stationId = row.station_id,
                            d.defectCode = row.defect_code,
                            d.defectSize = row.defect_size,
                            d.xCoord = row.x_coord,
                            d.yCoord = row.y_coord,
                            d.count = row.countnum,
                            d.chamberCleanCycles = row.cc,
                            d.defectTime = row.defect_time,
                            d.factoryCode = row.factory_code
                    """, batch=batch)
                    count += len(batch)
                    batch = []
                    pbar.update(count)

    # 处理剩余
    if batch:
        with driver.session() as session:
            session.run("""
                UNWIND $batch AS row
                MERGE (d:DefectRecord {defectId: row.defect_id})
                SET d.eventId = row.event_id,
                    d.glassId = row.glass_id,
                    d.lotId = row.lot_id,
                    d.productId = row.product_id,
                    d.eqpId = row.eqp_id,
                    d.chamber = row.chamber,
                    d.stationId = row.station_id,
                    d.defectCode = row.defect_code,
                    d.defectSize = row.defect_size,
                    d.xCoord = row.x_coord,
                    d.yCoord = row.y_coord,
                    d.count = row.countnum,
                    d.chamberCleanCycles = row.cc,
                    d.defectTime = row.defect_time,
                    d.factoryCode = row.factory_code
            """, batch=batch)
        count += len(batch)

    print(f"  Imported {count} defect points")
    return count

# ================== 关系创建 ==================

def create_relationships(driver):
    """创建节点之间的关系"""
    print("Creating relationships...")

    with driver.session() as session:
        # DefectRecord -> DefectEvent
        print("  Creating :PART_OF relationships...")
        session.run("""
            MATCH (d:DefectRecord)
            MATCH (e:DefectEvent {eventId: d.eventId})
            MERGE (d)-[:PART_OF]->(e)
        """)

        # DefectRecord -> CVDEquipment
        print("  Creating :OCCURS_AT relationships...")
        session.run("""
            MATCH (d:DefectRecord)
            MERGE (e:CVDEquipment {eqpId: d.eqpId})
            MERGE (d)-[:OCCURS_AT]->(e)
        """)

        # DefectRecord -> Glass
        print("  Creating :ON_GLASS relationships...")
        session.run("""
            MATCH (d:DefectRecord)
            MERGE (g:Glass {glassId: d.glassId})
            MERGE (d)-[:ON_GLASS]->(g)
        """)

        # DefectRecord -> Station
        print("  Creating :AT_STATION relationships...")
        session.run("""
            MATCH (d:DefectRecord)
            MERGE (s:Station {stationId: d.stationId})
            MERGE (d)-[:AT_STATION]->(s)
        """)

        # DefectCluster -> DefectEvent
        print("  Creating DefectCluster -> DefectEvent...")
        session.run("""
            MATCH (c:DefectCluster)
            MATCH (e:DefectEvent {eventId: c.eventId})
            MERGE (c)-[:BELONGS_TO]->(e)
        """)

    print("  All relationships created!")

# ================== 主函数 ==================

def main():
    parser = argparse.ArgumentParser(description="Import CVD data to Neo4j")
    parser.add_argument("--days", type=int, default=90, help="Days of history to import")
    parser.add_argument("--skip-defect-points", action="store_true", help="Skip defect_points import")
    args = parser.parse_args()

    import psycopg2

    print("=" * 60)
    print("CVD Knowledge Graph - Data Import")
    print("=" * 60)
    print()

    # 连接 PostgreSQL
    print("Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(**PG_CONFIG)
    pg_conn.autocommit = True
    cursor = pg_conn.cursor()
    print("  Connected!")

    # 连接 Neo4j
    print("Connecting to Neo4j...")
    driver = get_neo4j_driver()
    print("  Connected!")

    start_time = time.time()

    # 导入数据
    try:
        import_employees(driver, cursor)
        import_defect_classifications(driver, cursor)
        import_defect_events(driver, cursor, days=args.days)
        import_defect_clusters(driver, cursor, days=args.days)

        if not args.skip_defect_points:
            import_defect_points(driver, cursor, days=args.days)
        else:
            print("Skipping defect_points import...")

        create_relationships(driver)

    finally:
        driver.close()
        cursor.close()
        pg_conn.close()

    elapsed = time.time() - start_time
    print()
    print("=" * 60)
    print(f"Import completed in {elapsed:.1f} seconds!")
    print("=" * 60)

if __name__ == "__main__":
    main()
