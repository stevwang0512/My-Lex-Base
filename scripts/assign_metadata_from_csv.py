# [v0.40-meta-writer-base] ADD + scripts/assign_metadata_from_csv.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
assign_metadata_from_csv.py
- Read a CSV containing: title, category_code, category_name, level, catalog_key, sub_anchor, parent_key, version_key
- Unzip a content zip, write/update YAML front matter in each .md matched by title
- Repack to a new zip without exposing these keys to frontend (they stay only in .md front matter)
Usage:
  python scripts/assign_metadata_from_csv.py --zip content.zip --csv meta.csv --out content_with_meta.zip
"""
import argparse, csv, io, os, re, shutil, sys, tempfile, zipfile
from pathlib import Path

FRONT_KEYS = ["category_code","category_name","level","catalog_key","sub_anchor","parent_key","version_key"]

def load_csv(p: Path):
    rows = []
    with p.open("r", encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # normalize keys
            r = {k.strip(): (v.strip() if isinstance(v,str) else v) for k,v in r.items()}
            if not r.get("title"):
                continue
            rows.append(r)
    return rows

def slugify_title(t: str) -> str:
    s = re.sub(r"\s+", " ", t).strip()
    # loosen match: remove common punctuation for filename matching
    s = re.sub(r"[\\/:*?\"<>|·•，,。．、\-\u3000]+", " ", s)
    return s.lower()

def find_md_by_title(root: Path, title: str):
    target = slugify_title(title)
    cands = []
    for md in root.rglob("*.md"):
        base = md.stem.lower()
        norm = slugify_title(base)
        if norm == target or target in norm:
            cands.append(md)
    # prefer exact filename match
    if cands:
        cands.sort(key=lambda p: (p.stem.lower()!=title.lower(), len(p.stem)))
    return cands[0] if cands else None

def split_front_matter(text: str):
    if text.startswith("---"):
        parts = re.split(r"^---\\s*$|^\\.\\.\\.\\s*$", text, maxsplit=2, flags=re.M)
        # fallback simple split
        chunks = re.split(r"^---\\s*$", text, maxsplit=2, flags=re.M)
        if len(chunks)>=3:
            return ("yaml", chunks[1], chunks[2])
    return (None, None, text)

def parse_yaml(s: str):
    # minimal YAML parser for simple "key: value" lines
    data = {}
    if not s: return data
    for line in s.splitlines():
        m = re.match(r"\\s*([A-Za-z0-9_\\-]+)\\s*:\\s*(.*)\\s*$", line)
        if m:
            k,v = m.group(1), m.group(2)
            # strip quotes
            v = re.sub(r"^['\\\"]|['\\\"]$", "", v.strip())
            data[k] = v
    return data

def dump_yaml(d: dict):
    out = []
    for k, v in d.items():
        if v is None or v == "":
            continue
        sval = str(v)
        # quote if needed (contains spaces or YAML special chars)
        if re.search(r"[#:\\s>\-\[\]\{\}]", sval):
            # escape double quotes inside the value
            escaped = sval.replace('"', '\\"')
            out.append(f'{k}: "{escaped}"')
        else:
            out.append(f"{k}: {sval}")
    return "\n".join(out) + "\n"

def ensure_front_matter(md_path: Path, assign: dict):
    text = md_path.read_text("utf-8", errors="ignore")
    kind, yaml_text, body = split_front_matter(text)
    if kind == "yaml":
        meta = parse_yaml(yaml_text)
    else:
        meta, body = {}, text
    # apply updates
    for k in FRONT_KEYS + ["title"]:
        if k in assign and assign[k] not in (None, ""):
            meta[k] = assign[k]
    # rebuild
    new = "---\n" + dump_yaml(meta) + "---\n" + body.lstrip()
    md_path.write_text(new, "utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True, help="zip containing content/ directory")
    ap.add_argument("--csv", required=True, help="CSV with metadata (must contain 'title' column)")
    ap.add_argument("--out", required=False, default="content_with_meta.zip")
    ap.add_argument("--content-root", default="content", help="content dir inside zip (default: content)")
    args = ap.parse_args()

    zipp = Path(args.zip)
    csvp = Path(args.csv)
    outp = Path(args.out)

    if not zipp.exists(): 
        print("[error] zip not found:", zipp); sys.exit(1)
    if not csvp.exists():
        print("[error] csv not found:", csvp); sys.exit(1)

    rows = load_csv(csvp)
    if not rows:
        print("[error] empty csv"); sys.exit(1)

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        with zipfile.ZipFile(zipp, "r") as zf:
            zf.extractall(td)

        root = td / args.content_root
        if not root.exists():
            print("[error] content root not found in zip:", root); sys.exit(1)

        # Build in-memory title catalog with normalized keys
        def normalize(s: str) -> str:
            import unicodedata
            if s is None: return ""
            s = unicodedata.normalize('NFKC', s)
            s = re.sub(r"\s+", " ", s).strip().lower()
            s = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "", s)
            return s

        title_items = []
        for idx, r in enumerate(rows):
            title = r.get("title") or r.get("Title") or r.get("名称") or r.get("文件名")
            if not title:
                continue
            cat = (r.get("category_code") or "").lstrip('0')
            title_items.append({"idx": idx, "row": r, "title": title, "norm": normalize(title), "category": cat})

        # list md files in the extracted tree
        md_files = list((td / args.content_root).rglob("*.md"))

        # candidates for each md
        md_candidates = {}
        for md in md_files:
            md_norm = normalize(md.stem)
            # derive md category from path like 'content/02 民法/...'
            parts = md.parts
            md_cat = ""
            if len(parts) >= 2 and parts[0] == args.content_root:
                m = re.match(r"^(\d+)", parts[1])
                if m:
                    md_cat = m.group(1).lstrip('0')

            cands = []
            # prefer same-category titles first
            same_cat = [t for t in title_items if t["category"] == md_cat] if md_cat else []
            search_space = same_cat if same_cat else title_items
            for t in search_space:
                # match if normalized equal or contained
                if t["norm"] == md_norm or t["norm"] in md_norm or md_norm in t["norm"]:
                    # scoring: exact match wins; category match +100; length proximity bonus
                    score = 0
                    if t["norm"] == md_norm:
                        score += 1000
                    else:
                        score += 500
                    if t["category"] == md_cat and md_cat:
                        score += 100
                    score += max(0, 100 - abs(len(t["norm"]) - len(md_norm)))
                    cands.append((score, t))
            # if no same-category candidates, try global search (if not already)
            if not cands and search_space is not title_items:
                for t in title_items:
                    if t["norm"] == md_norm or t["norm"] in md_norm or md_norm in t["norm"]:
                        score = 0
                        if t["norm"] == md_norm:
                            score += 1000
                        else:
                            score += 500
                        if t["category"] == md_cat and md_cat:
                            score += 100
                        score += max(0, 100 - abs(len(t["norm"]) - len(md_norm)))
                        cands.append((score, t))

            # sort candidates by score descending
            cands.sort(key=lambda x: (-x[0], -len(x[1]["norm"]) ))
            md_candidates[str(md)] = cands

        # Greedy assignment to ensure one-to-one mapping
        assigned = {}
        used_title_idxs = set()
        # sort md files by top candidate strength so confident matches are assigned first
        md_order = sorted(md_candidates.items(), key=lambda kv: (-(kv[1][0][0] if kv[1] else 0)))
        for md_path, cands in md_order:
            sel = None
            for score, t in cands:
                if t["idx"] not in used_title_idxs:
                    sel = t
                    used_title_idxs.add(t["idx"])
                    break
            assigned[md_path] = sel

        # apply assignments
        ok = 0
        miss = []
        ambiguous = []
        for md_path, sel in assigned.items():
            if not sel:
                miss.append(md_path)
                continue
            # build assign dict from selected title row
            r = sel["row"]
            assign = {
                "title": sel["title"],
                "category_code": r.get("category_code") or r.get("category") or r.get("类目编码"),
                "category_name": r.get("category_name") or r.get("类目名称"),
                "level": r.get("level") or r.get("层级"),
                "catalog_key": r.get("catalog_key") or r.get("目录键"),
                "sub_anchor": r.get("sub_anchor") or r.get("子锚点"),
                "parent_key": r.get("parent_key") or r.get("上级键"),
                "version_key": r.get("version_key") or r.get("版本键"),
            }
            ensure_front_matter(Path(md_path), assign)
            ok += 1
            # record ambiguous if there were multiple candidates
            if len(md_candidates.get(md_path, [])) > 1:
                ambiguous.append((md_path, [entry[1]["title"] for entry in md_candidates[md_path]]))

        # re-pack
        with zipfile.ZipFile(outp, "w", compression=zipfile.ZIP_DEFLATED) as zw:
            for p in td.rglob("*"):
                if p.is_file():
                    zw.write(p, arcname=str(p.relative_to(td)))

        print(f"[ok] updated md files: {ok}, missing (unassigned md): {len(miss)}")
        if miss:
            print("[miss] md files not assigned:")
            for m in miss[:30]:
                print(" -", m)
        if ambiguous:
            print(f"[warn] ambiguous mappings (md -> candidate titles), count: {len(ambiguous)}")
            for mdp, cand in ambiguous[:30]:
                print(mdp)
                for t in cand:
                    print("  -", t)

if __name__ == "__main__":
    main()
