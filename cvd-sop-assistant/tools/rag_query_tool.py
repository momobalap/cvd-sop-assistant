"""
RAG 向量库查询工具
"""
import os
import requests
import chromadb

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "herald/dmeta-embedding-zh:latest")
CHROMA_PATH = os.environ.get("CHROMA_PATH", os.path.join(os.path.dirname(__file__), "../../cvd-kg/chroma_sop_v3"))
COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "sop_full")

def get_embedding(texts):
    return [requests.post(
        f"{OLLAMA_URL}/api/embeddings",
        json={"model": EMBED_MODEL, "prompt": t},
        timeout=60
    ).json()["embedding"] for t in texts]

def query(question: str, top_k: int = 4) -> str:
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    col = client.get_collection(COLLECTION_NAME)
    emb = get_embedding([question])[0]
    results = col.query(query_embeddings=[emb], n_results=top_k)

    output = "【RAG - 相关 SOP 内容】\n"
    for i, (doc, meta, dist) in enumerate(zip(
        results["documents"], results["metadatas"], results["distances"]
    )):
        output += f"\n--- 结果 {i+1} (相关度:{1-dist[0]:.2f}) ---\n"
        output += f"来源: {meta[0]['source']} | 类型: {meta[0]['type']}\n"
        output += f"{doc[0]}\n"
    return output

if __name__ == "__main__":
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else "薄膜机台异常处理流程"
    print(query(q))
