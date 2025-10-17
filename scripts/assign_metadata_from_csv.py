#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
assign_metadata_from_csv.py — v0.40-meta-writer-base (Always-emit keys)
- Read a CSV containing: title, category_code, category_name, level, catalog_key, sub_anchor, parent_key, version_key
- Unzip a content zip, write/update YAML front matter in each .md matched by title (FULL/HALF width insensitive, punctuation-agnostic)
- Idempotent on content; **always emits FRONT_KEYS even when values are empty** (e.g., version_key, sub_anchor)
- De-duplicates CSV rows by a normalized (title, category_code) key unless --no-dedupe is provided
- Emits a mapping report and ambiguity diagnostics

Usage:
  python scripts/assign_metadata_from_csv.py --zip content.zip --csv meta.csv --out content_with_meta.zip [--content-root content] [--dry-run]
"""
from __future__ import annotations
import argparse, csv, io, re, sys, tempfile, zipfile
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
    if text.startswith("---"):
        m = re.match(r"^---\s*\n(.*?)\n---\s*\n(.*)\Z", text, flags=re.S)
        if m:
            return ("yaml", m.group(1), m.group(2))
        parts = re.split(r"^---\s*$", text, maxsplit=2, flags=re.M)
        if len(parts) >= 3:
            return ("yaml", parts[1], parts[2])
    return (None, None, text)


def parse_yaml(s: str):
    data = {}
    if not s:
        return data
    for line in s.splitlines():
        m = re.match(r"\s*([A-Za-z0-9_\-]+)\s*:\s*(.*)\s*$", line)
        if m:
            k, v = m.group(1), m.group(2)
            v = re.sub(r"^['\"]|['\"]$", "", v.strip())
            data[k] = v
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

    new_text = "---\n" + dump_yaml(meta) + "---\n" + body.lstrip()

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
    if len(parts) >= 2 and parts[0] == content_root:
        m = re.match(r"^(\d+)", parts[1])
        if m:
            md_cat = m.group(1).lstrip("0")
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
    ap.add_argument("--zip", required=True, help="zip containing content/ directory")
    ap.add_argument("--csv", required=True, help="CSV with metadata (must contain 'title' column)")
    ap.add_argument("--out", default="content_with_meta.zip")
    ap.add_argument("--content-root", default="content")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--dedupe", choices=["first","last","none"], default="first")
    args = ap.parse_args()

    zipp = Path(args.zip)
    csvp = Path(args.csv)
    outp = Path(args.out)

    if not zipp.exists():
        print("[error] zip not found:", zipp); sys.exit(1)
    if not csvp.exists():
        print("[error] csv not found:", csvp); sys.exit(1)

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

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        with zipfile.ZipFile(zipp, "r") as zf:
            zf.extractall(td)

        root = td / args.content_root
        if not root.exists():
            print("[error] content root not found in zip:", root); sys.exit(1)

        md_files = list(root.rglob("*.md"))
        md_candidates = {str(md): best_candidates_for_md(md, title_items, args.content_root) for md in md_files}

        assigned = {}
        used_title_idxs = set()
        md_order = sorted(md_candidates.items(), key=lambda kv: (-(kv[1][0][0] if kv[1] else 0)))
        for md_path, cands in md_order:
            sel = None
            for score, t in cands:
                if t["idx"] not in used_title_idxs:
                    sel = t
                    used_title_idxs.add(t["idx"])
                    break
            assigned[md_path] = sel

        ok = 0
        skipped_no_change = 0
        miss = []
        ambiguous = []
        mapping_report = []

        for md_path, sel in assigned.items():
            if not sel:
                miss.append(md_path); continue
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
                changed = True
            else:
                changed, _ = ensure_front_matter(Path(md_path), assign)
            if changed:
                ok += 1
            else:
                skipped_no_change += 1
            mapping_report.append((md_path, sel["title"], sel["category"], changed))
            if len(md_candidates.get(md_path, [])) > 1:
                ambiguous.append((md_path, [entry[1]["title"] for entry in md_candidates[md_path]]))

        if not args.dry_run:
            with zipfile.ZipFile(outp, "w", compression=zipfile.ZIP_DEFLATED) as zw:
                for p in (td).rglob("*"):
                    if p.is_file():
                        zw.write(p, arcname=str(p.relative_to(td)))

        print(f"[ok] updated md files: {ok}, unchanged: {skipped_no_change}, missing (unassigned md): {len(miss)}")
        if args.dry_run:
            print("[note] dry-run mode: no files written, no zip produced.")
        if miss:
            print("[miss] md files not assigned:")
            for m in miss[:50]:
                print(" -", m)
        if ambiguous:
            print(f"[warn] ambiguous mappings (md -> candidate titles), count: {len(ambiguous)}")
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
            sys.stdout.write(out.getvalue())
        except Exception as e:
            print("[warn] failed to emit mapping report:", e)

if __name__ == "__main__":
    main()
