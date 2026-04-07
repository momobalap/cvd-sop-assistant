# CVD SOP Assistant - 前端界面

基于 Neo4j 图谱 + RAG 向量库的 CVD 薄膜机台异常处理智能问答界面。

![界面预览](https://raw.githubusercontent.com/momobalap/cvd-sop-assistant/main/public/preview.png)

## 功能

- 🎨 视觉效果抓眼球的深色主题 Web UI
- 🔍 自动路由判断（问原因 → Neo4j / 问流程 → RAG）
- 📊 合并 Neo4j 图谱 + SOP 文档结果
- 🤖 LLM 智能润色回答

## 快速开始

### 1. 安装依赖

```bash
# 前端
npm install

# 后端 Python 依赖
pip3 install flask flask-cors requests neo4j
```

### 2. 启动后端

```bash
python3 server.py
```

### 3. 启动前端（新终端）

```bash
npm run dev
```

访问 http://localhost:5173

## 项目结构

```
cvd-sop-assistant/
├── src/                    # React + TypeScript 前端
│   ├── App.tsx             # 主界面
│   └── index.css           # 样式
├── tools/                  # 后端工具
│   ├── neo4j_query_tool.py  # Neo4j 图谱查询
│   ├── rag_query_tool.py    # RAG 向量检索
│   └── polish_tool.py       # LLM 润色
├── server.py               # Flask API 服务
├── vite.config.ts          # Vite 配置
└── package.json
```

## 数据后端

- **Neo4j**: bolt://localhost:17687 (neo4j / password)
- **Chroma SOP**: cvd-kg/chroma_sop_v3 (193 chunks)
- **Ollama**: http://localhost:11434 (qwen3:4b)

## 部署

需要先准备好后端数据服务（Neo4j + Chroma + Ollama），然后：

```bash
npm run build
# 打包到 dist/
```
