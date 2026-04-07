#!/bin/bash
# Neo4j CVD Data Import via HTTP API
URL="http://localhost:17474/db/neo4j/tx/commit"
AUTH="neo4j:password"

# Import DefectSOPRule nodes
import_defect() {
  curl -s -u $AUTH -X POST $URL -H "Content-Type: application/json" \
    -d "{\"statements\":[{\"statement\":\"CREATE (d:DefectSOPRule {defect_code:\"$1\", defect_name:\"$2\", category:\"$3\", hazard_level:\"$4\", hazard_threshold:\"$5\", description:\"$6\"})\"}]}" > /dev/null
  echo "✓ $1"
}

echo "=== Importing CVD DefectSOPRule nodes ==="
import_defect "FC01" "完全沒有膜沉積" "CVD" "重大异常" "≥1片" "在MACO会看见无CVD膜色，TEF量测膜厚为0"
import_defect "FC02" "CVD 破洞" "CVD" "重大异常" "≥1片" "破洞周围有明显彩晕"
import_defect "FC03" "大Particle" "CVD" "重大异常" "≥1片" "大Particle分成On Film及In Film"
import_defect "FC04" "CVD Splash" "CVD" "重大异常" "≥3片" "金属溅散于玻璃表面"
import_defect "FC07" "聚集性Particle" "CVD" "重大异常" "≥1片" "Particle聚集对Yield杀伤力大"
import_defect "FC08" "纖維狀Particle" "CVD" "重大异常" "≥1片" "形成黑色胶状物黏着"
import_defect "FC09" "沙狀Defect" "CVD" "严重异常" "≥1片" "小Particle呈放射性沙状"
import_defect "FC11" "CVD Arcing" "CVD" "重大异常" "≥2片" "膜表面有焦黑痕迹"
import_defect "FC14" "CVD膜厚/均匀度异常" "CVD" "严重异常" "≥1片" "SPC OOS/OOC需停机"
import_defect "FC93" "CVD ENG人为疏失" "CVD" "重大异常" "≥1片" "ENG处理设备或产品疏忽"

echo ""
echo "=== Importing PVD DefectSOPRule nodes ==="
import_defect "S004" "異常放電 Arcing" "PVD" "重大异常" "≥1片" "膜表面有焦黑痕迹"
import_defect "S05G" "Gate SPLASH" "PVD" "严重异常" "≥1片" "Gate位置金属溅散"
import_defect "S10G" "Gate 破洞" "PVD" "重大异常" "≥1片" "In Film Particles留下破洞"
import_defect "S10S" "S/D 破洞" "PVD" "重大异常" "≥1片" "第三层S/D破洞"
import_defect "S013" "Metal 阻值超规" "PVD" "严重异常" "≥1片" "RS值整体偏高或U%不良"
import_defect "S014" "Gate Particle过多" "PVD" "严重异常" "≥1片" "In Film Particle形成膜破洞"
import_defect "S015" "S/D Particle过多" "PVD" "严重异常" "≥1片" "S/D位置Particle"
import_defect "S023" "聚集性Particle" "PVD" "重大异常" "≥1片" "聚集性Particle"
import_defect "S030" "M1 点狀鋁咬" "PVD" "严重异常" "≥1片" "沿双眼皮处半圆形咬痕"

echo ""
echo "=== Importing Relationships ==="
# FC11 -> requires_inspection -> TOI
curl -s -u $AUTH -X POST $URL -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"MATCH (d:DefectSOPRule {defect_code:\"FC11\"}), (i:Inspection {code:\"TOI\"}) CREATE (d)-[:REQUIRES_INSPECTION]->(i)"}]}' > /dev/null && echo "✓ FC11 -> TOI"
curl -s -u $AUTH -X POST $URL -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"MATCH (d:DefectSOPRule {defect_code:\"FC11\"}), (i:Inspection {code:\"TMM\"}) CREATE (d)-[:REQUIRES_INSPECTION]->(i)"}]}' > /dev/null && echo "✓ FC11 -> TMM"
curl -s -u $AUTH -X POST $URL -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"MATCH (d:DefectSOPRule {defect_code:\"FC11\"}), (i:Inspection {code:\"TEF\"}) CREATE (d)-[:REQUIRES_INSPECTION]->(i)"}]}' > /dev/null && echo "✓ FC11 -> TEF"

# FC11 -> linked_alarm -> RF Detection
curl -s -u $AUTH -X POST $URL -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"MATCH (d:DefectSOPRule {defect_code:\"FC11\"}), (a:AlarmCode {code:\"RF Detection\"}) CREATE (d)-[:LINKED_ALARM]->(a)"}]}' > /dev/null && echo "✓ FC11 -> RF Detection"

echo ""
echo "=== Done! ==="
