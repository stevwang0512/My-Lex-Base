#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_site.py
- 增量同步 content/ -> site/content/
- 扫描所有 .md，生成 site/index/docs.json（id/path/title）
- 使用 jieba 构建简单全文索引，生成 site/index/{route.json, shard_all.json}
后续可扩展：索引分片、预渲染 HTML、指纹化等
"""
import os, re, json, hashlib, shutil
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "content"
DST = ROOT / "site" / "content"
IDX = ROOT / "site" / "index"
IDX.mkdir(parents=True, exist_ok=True)

def read_file(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="ignore")

def strip_front_matter(md: str) -> str:
    m = re.match(r"^---[\s\S]*?---\n?", md)
    return md[m.end():] if m else md

def first_heading_title(md: str, fallback: str) -> str:
    m = re.search(r"^\s*#\s+(.+)$", md, flags=re.M)
    return (m.group(1).strip() if m else fallback)

def slugify(text: str) -> str:
    s = re.sub(r"[^\w\u4e00-\u9fa5]+", "-", text.strip().lower())
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "sec"

def copy_content_incremental(src: Path, dst: Path):
    if not src.exists():
        raise SystemExit(f"[build] ERROR: {src} 不存在。请将 Markdown 放在仓库根的 content/ 下。")
    dst.mkdir(parents=True, exist_ok=True)
    for p in src.rglob("*"):
        rel = p.relative_to(src)
        out = dst / rel
        if p.is_dir():
            out.mkdir(parents=True, exist_ok=True)
            continue
        if not out.exists() or p.stat().st_mtime_ns > out.stat().st_mtime_ns:
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, out)

def tokenize(text: str):
    try:
        import jieba
    except ImportError:
        print("[build] WARN: 未安装 jieba，跳过中文分词索引构建。运行 `pip install jieba` 以开启。")
        for tok in re.findall(r"[\w\u4e00-\u9fa5]+", text):
            if tok.strip():
                yield tok
        return
    for tok in jieba.cut_for_search(text):
        tok = tok.strip()
        if tok:
            yield tok

def build():
    # 1) 同步 content -> site/content
    copy_content_incremental(SRC, DST)

    # 2) 扫描文档清单
    docs = []
    doc_bodies = {}
    id_by_path = {}
    for p in sorted(SRC.rglob("*.md")):
        rel = p.relative_to(SRC).as_posix()
        raw = read_file(p)
        body = strip_front_matter(raw)
        title = first_heading_title(body, rel)
        doc_id = hashlib.md5(rel.encode("utf-8")).hexdigest()[:12]
        docs.append({"id": doc_id, "path": rel, "title": title})
        doc_bodies[doc_id] = body
        id_by_path[rel] = doc_id

    (IDX / "docs.json").write_text(json.dumps({"docs": docs}, ensure_ascii=False, indent=2), "utf-8")

    # 3) 构建 MVP 倒排索引（大规模时建议分片）
    inverted = defaultdict(Counter)  # token -> Counter({doc_id: tf})
    for d in docs:
        doc_id = d["id"]
        text = re.sub(r"`[^`]+`", " ", doc_bodies[doc_id])
        text = re.sub(r"#+\s*", " ", text)
        text = re.sub(r"\*{1,2}", " ", text)
        for t in tokenize(text):
            inverted[t][doc_id] += 1

    postings = {t: [[doc_id, int(tf)] for doc_id, tf in inv.items()] for t, inv in inverted.items()}
    (IDX / "shard_all.json").write_text(json.dumps({"postings": postings}, ensure_ascii=False), "utf-8")
    (IDX / "route.json").write_text(json.dumps({"mode": "single", "files": ["shard_all.json"]}, ensure_ascii=False), "utf-8")

    print(f"[build] 文档数: {len(docs)}, 词项数: {len(inverted)}")
    print("[build] 输出: site/content/, site/index/docs.json, site/index/shard_all.json, site/index/route.json")

if __name__ == "__main__":
    build()
