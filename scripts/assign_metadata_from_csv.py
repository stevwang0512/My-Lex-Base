#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
assign_metadata_from_csv.py — v0.41-meta-writer-inplace (Always-emit keys, In-place modification)
- Read a CSV containing: title, category_code, category_name, level, catalog_key, sub_anchor, parent_key, version_key
- Directly find and update YAML front matter in each .md file matched by title (in-place).
- Idempotent: a robust parser ensures only one YAML block exists, cleaning up duplicates.
- De-duplicates CSV rows by a normalized (title, category_code) key unless --no-dedupe is provided.
- Emits a mapping report and ambiguity diagnostics.

Usage:
  python scripts/assign_metadata_from_csv.py --csv meta.csv [--content-dir content] [--dry-run]
"""
from __future__ import annotations
import argparse
import csv
import io
import re
import sys
from pathlib import Path
import unicodedata

FRONT_KEYS = [
    "category_code", "category_name", "level", "catalog_key", "sub_anchor", "parent_key", "version_key"
]

# -------------------------------
# Normalization helpers (SOP):
# - NFKC to fold FULL/HALF width + compatibility chars
# - Lower-case, strip whitespace
# - Remove non (ASCII alnum + CJK)
# -------------------------------
ALNUM_CJK_RE = re.compile(r"[^0-9a-zA-Z\u4e00-\u9fff]+", re.UNICODE)

def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)

def normalize_title(s: str) -> str:
    if s is None:
        return ""
    s = nfkc(s).casefold().strip()
    s = re.sub(r"\s+", " ", s)
    s = ALNUM_CJK_RE.sub("", s)
    return s

# -------------------------------
# CSV IO
# -------------------------------

def load_csv(p: Path):
    rows = []
    with p.open("r", encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.DictReader(f)
        for r in reader:
            r = { (k or "").strip(): (v.strip() if isinstance(v,str) else v) for k,v in r.items() }
            title = r.get("title") or r.get("Title") or r.get("名称") or r.get("文件名")
            if not title:
                continue
            r["title"] = title
            rows.append(r)
    return rows

def dedupe_rows(rows, keep_last=False):
    seen = {}
    order = []
    for i, r in enumerate(rows):
        title = r.get("title")
        cat = (r.get("category_code") or r.get("category") or r.get("类目编码") or "").lstrip("0")
        key = (normalize_title(title), cat)
        if keep_last:
            seen[key] = i
        else:
            if key not in seen:
                seen[key] = i
                order.append(i)
    if keep_last:
        idxs = sorted(seen.values())
        return [rows[i] for i in idxs]
    return [rows[i] for i in order]

# -------------------------------
# Markdown front matter utilities
# -------------------------------

def split_front_matter(text: str):
    """
    Robustly splits YAML front matter from body. Only recognizes the first block.
    This inherently cleans up any previously duplicated YAML blocks upon rewrite.
    """
    if not text.startswith('---'):
        return None, None, text

    # Find the end of the first YAML block
    try:
        # Start searching after the initial '---'
        end_marker_pos = text.find('\n---', 4)
        if end_marker_pos == -1:
            return None, None, text # No closing '---' found

        yaml_text = text[4:end_marker_pos].strip()
        # The rest of the file is the body, including any potential duplicate blocks
        body = text[end_marker_pos + 4:].lstrip()
        return "yaml", yaml_text, body
    except Exception:
        return None, None, text


def parse_yaml(s: str):
    data = {}
    if not s:
        return data
    for line in s.splitlines():
        if ':' in line:
            key, val = line.split(':', 1)
            key = key.strip()
            # Clean up value: remove surrounding quotes and whitespace
            val = val.strip().strip('"').strip("'")
            data[key] = val
    return data


def dump_yaml(d: dict):
    out = []
    # Always emit keys in fixed order; empty values become ""
    for k in ["title"] + FRONT_KEYS:
        v = d.get(k, "")
        sval = "" if v is None else str(v)
        if sval == "" or re.search(r"[#:\\s>\\-\\[\\]\\{\\}]", sval):
            sval = sval.replace('"', '\\"')
            out.append(f'{k}: "{sval}"')
        else:
            out.append(f"{k}: {sval}")
    return "\n".join(out) + "\n"


def ensure_front_matter(md_path: Path, assign: dict) -> tuple[bool, str]:
    """Return (changed, new_text) and only write if changed.
    Ensures FRONT_KEYS are always present; empty values become "".
    """
    text = md_path.read_text("utf-8", errors="ignore")
    kind, yaml_text, body = split_front_matter(text)

    if kind == "yaml":
        meta = parse_yaml(yaml_text)
    else:
        meta, body = {}, text

    changed = False

    # Title: only update if provided and non-empty
    if assign.get("title"):
        if meta.get("title") != assign["title"]:
            meta["title"] = assign["title"]
            changed = True

    # Other keys: always ensure presence; empty allowed and emitted
    for k in FRONT_KEYS:
        new_val = assign.get(k, "")
        old_val = meta.get(k)
        if (old_val or "") != (new_val or ""):
            meta[k] = new_val or ""
            changed = True
        else:
            if k not in meta:
                meta[k] = ""
                changed = True

    new_text = "---\n" + dump_yaml(meta) + "---\n" + body

    if changed and new_text != text:
        md_path.write_text(new_text, "utf-8")
        return True, new_text
    return False, text

# -------------------------------
# Matching & assignment
# -------------------------------

def build_title_items(rows):
    items = []
    for idx, r in enumerate(rows):
        title = r.get("title")
        if not title:
            continue
        cat = (r.get("category_code") or r.get("category") or r.get("类目编码") or "").lstrip("0")
        items.append({
            "idx": idx,
            "row": r,
            "title": title,
            "norm": normalize_title(title),
            "category": cat,
        })
    return items


def best_candidates_for_md(md: Path, title_items, content_root: str):
    md_norm = normalize_title(md.stem)
    parts = md.parts
    md_cat = ""
    # Adjust path parsing to be more flexible
    try:
        root_index = parts.index(content_root)
        if root_index < len(parts) - 1:
            m = re.match(r"^(\d+)", parts[root_index + 1])
            if m:
                md_cat = m.group(1).lstrip("0")
    except ValueError:
        pass # content_root not in path, md_cat remains empty

    cands = []
    for t in title_items:
        if not t["norm"] or not md_norm:
            continue
        if t["norm"] == md_norm or t["norm"] in md_norm or md_norm in t["norm"]:
            score = 500
            if t["norm"] == md_norm:
                score += 500
            if t["category"] == md_cat and md_cat:
                score += 100
            score += max(0, 100 - abs(len(t["norm"]) - len(md_norm)))
            cands.append((score, t))
    cands.sort(key=lambda x: (-x[0], -len(x[1]["norm"])))
    return cands

# -------------------------------
# Main
# -------------------------------

def main():
    ap = argparse.ArgumentParser()
    # MODIFIED: Removed --zip and --out, added --content-dir for in-place modification
    ap.add_argument("--csv", required=True, help="CSV with metadata (must contain 'title' column)")
    ap.add_argument("--content-dir", default="content", help="Path to the content/ directory to modify in-place")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--dedupe", choices=["first","last","none"], default="first")
    args = ap.parse_args()

    csvp = Path(args.csv)
    content_dir = Path(args.content_dir)

    if not content_dir.is_dir():
        print(f"[error] content directory not found: {content_dir}"); sys.exit(1)
    if not csvp.exists():
        print(f"[error] csv not found: {csvp}"); sys.exit(1)

    rows0 = load_csv(csvp)
    if not rows0:
        print("[error] empty csv"); sys.exit(1)

    if args.dedupe == "first":
        rows = dedupe_rows(rows0, keep_last=False)
    elif args.dedupe == "last":
        rows = dedupe_rows(rows0, keep_last=True)
    else:
        rows = rows0

    title_items = build_title_items(rows)
    
    # MODIFIED: No longer uses tempdir or zip files. Works directly on the content directory.
    print(f"[info] Operating directly on directory: {content_dir.resolve()}")

    md_files = list(content_dir.rglob("*.md"))
    # The content_root name is needed for path-based category matching
    content_root_name = content_dir.name
    md_candidates = {str(md): best_candidates_for_md(md, title_items, content_root_name) for md in md_files}

    assigned = {}
    used_title_idxs = set()
    md_order = sorted(md_candidates.items(), key=lambda kv: (-(kv[1][0][0] if kv[1] else 0)))
    for md_path_str, cands in md_order:
        sel = None
        for score, t in cands:
            if t["idx"] not in used_title_idxs:
                sel = t
                used_title_idxs.add(t["idx"])
                break
        assigned[md_path_str] = sel

    ok = 0
    skipped_no_change = 0
    miss = []
    ambiguous = []
    mapping_report = []

    for md_path_str, sel in assigned.items():
        md_path = Path(md_path_str)
        if not sel:
            miss.append(str(md_path.relative_to(content_dir.parent))); continue
        r = sel["row"]
        assign = {
            "title": sel["title"],
            "category_code": r.get("category_code") or r.get("category") or r.get("类目编码") or "",
            "category_name": r.get("category_name") or r.get("类目名称") or "",
            "level": r.get("level") or r.get("层级") or "",
            "catalog_key": r.get("catalog_key") or r.get("目录键") or "",
            "sub_anchor": r.get("sub_anchor") or r.get("子锚点") or "",
            "parent_key": r.get("parent_key") or r.get("上级键") or "",
            "version_key": r.get("version_key") or r.get("版本键") or "",
        }
        if args.dry_run:
            changed = True # Simulate a change for reporting purposes
        else:
            changed, _ = ensure_front_matter(md_path, assign)
        if changed:
            ok += 1
        else:
            skipped_no_change += 1
        
        report_path = str(md_path.relative_to(content_dir.parent))
        mapping_report.append((report_path, sel["title"], sel["category"], changed))
        
        if len(md_candidates.get(md_path_str, [])) > 1:
            ambiguous.append((report_path, [entry[1]["title"] for entry in md_candidates[md_path_str]]))

    # MODIFIED: Removed the final zip creation step.
    print(f"[ok] updated md files: {ok}, unchanged: {skipped_no_change}, missing (unassigned md): {len(miss)}")
    if args.dry_run:
        print("[note] dry-run mode: no files were written.")
    else:
        print("[info] Files were modified in-place.")

    if miss:
        print("\n[miss] md files not assigned:")
        for m in miss[:50]:
            print(" -", m)
    if ambiguous:
        print(f"\n[warn] ambiguous mappings (md -> candidate titles), count: {len(ambiguous)}")
        for mdp, cand in ambiguous[:50]:
            print(mdp)
            for t in cand:
                print("  -", t)
    
    # Mapping report CSV to stdout
    try:
        import csv as _csv
        out = io.StringIO()
        w = _csv.writer(out)
        w.writerow(["md_path","matched_title","category","changed"])
        for mdp, t, c, ch in mapping_report:
            w.writerow([mdp, t, c, "yes" if ch else "no"])
        print("\n--- Mapping Report (CSV) ---")
        sys.stdout.write(out.getvalue())
    except Exception as e:
        print("[warn] failed to emit mapping report:", e)

if __name__ == "__main__":
    main()