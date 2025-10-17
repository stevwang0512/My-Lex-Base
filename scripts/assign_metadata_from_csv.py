#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
assign_metadata_from_csv.py — v0.40-meta-writer-base (SOP-ready)
- Read a CSV containing: title, category_code, category_name, level, catalog_key, sub_anchor, parent_key, version_key
- Unzip a content zip, write/update YAML front matter in each .md matched by title (FULL/HALF width insensitive, punctuation-agnostic)
- Idempotent: only writes a file if content would change; avoids duplicated applications of the same CSV row
- De-duplicates CSV rows by a normalized (title, category_code) key unless --no-dedupe is provided
- Emits a mapping report and ambiguity diagnostics

Usage:
  python scripts/assign_metadata_from_csv.py --zip content.zip --csv meta.csv --out content_with_meta.zip [--content-root content] [--dry-run]
"""
from __future__ import annotations
import argparse, csv, io, os, re, shutil, sys, tempfile, zipfile
from pathlib import Path
import unicodedata

FRONT_KEYS = [
    "category_code","category_name","level","catalog_key","sub_anchor","parent_key","version_key"
]

# -------------------------------
# Normalization helpers (SOP):
# - NFKC to fold FULL/HALF width + compatibility chars
# - Lower-case, strip whitespace
# - Remove punctuation and common separators
# - Collapse spaces
# -------------------------------
PUNCT_RE = re.compile(r"[\s\-_/\\·•，,。．、：:；;！!？?（）()［\[\]］【】<>""'`|~^+*=…—–－·]+", re.UNICODE)
ALNUM_CJK_RE = re.compile(r"[^0-9a-zA-Z\u4e00-\u9fff]+", re.UNICODE)

def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)

def normalize_title(s: str) -> str:
    if s is None:
        return ""
    s = nfkc(s).casefold().strip()
    s = re.sub(r"\s+", " ", s)
    # Keep CJK + ASCII letters/digits only for matching
    s = ALNUM_CJK_RE.sub("", s)
    return s

# For filename-friendly slug (used only for loose comparisons)
def slugify_title(s: str) -> str:
    if s is None:
        return ""
    s = nfkc(s).casefold().strip()
    s = PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s)
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
            # flexible title field names (CN friendly)
            title = r.get("title") or r.get("Title") or r.get("名称") or r.get("文件名")
            if not title:
                continue
            r["title"] = title
            rows.append(r)
    return rows


def dedupe_rows(rows, key_fields=("title","category_code")):
    """De-duplicate rows by normalized key (title, category_code).
    Keeps the *first* occurrence by default to avoid repeated writes; override with --dedupe=last.
    """
    seen = {}
    order = []
    for i, r in enumerate(rows):
        title = r.get("title")
        cat = (r.get("category_code") or r.get("category") or r.get("类目编码") or "").lstrip("0")
        key = (normalize_title(title), cat)
        if key not in seen:
            seen[key] = i
            order.append(i)
    return [rows[i] for i in order]


def dedupe_rows_last(rows, key_fields=("title","category_code")):
    seen = {}
    for i, r in enumerate(rows):
        title = r.get("title")
        cat = (r.get("category_code") or r.get("category") or r.get("类目编码") or "").lstrip("0")
        key = (normalize_title(title), cat)
        seen[key] = i
    # keep last
    idxs = sorted(seen.values())
    return [rows[i] for i in idxs]

# -------------------------------
# Markdown front matter utilities
# -------------------------------

def split_front_matter(text: str):
    if text.startswith("---"):
        # Try strict YAML fence first
        m = re.match(r"^---\s*\n(.*?)\n---\s*\n(.*)\Z", text, flags=re.S)
        if m:
            return ("yaml", m.group(1), m.group(2))
        # Fallback loose match
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
    for k in ["title"] + FRONT_KEYS:
        if k not in d:
            continue
        v = d.get(k)
        if v is None or v == "":
            continue
        sval = str(v)
        if re.search(r"[#:\s>\-\[\]\{\}]", sval):
            sval = sval.replace('"', '\\"')
            out.append(f'{k}: "{sval}"')
        else:
            out.append(f"{k}: {sval}")
    return "\n".join(out) + "\n"


def ensure_front_matter(md_path: Path, assign: dict) -> tuple[bool, str]:
    """Return (changed, new_text) and only write if changed."""
    text = md_path.read_text("utf-8", errors="ignore")
    kind, yaml_text, body = split_front_matter(text)

    if kind == "yaml":
        meta = parse_yaml(yaml_text)
    else:
        meta, body = {}, text

    # apply updates (idempotent)
    changed = False
    for k in FRONT_KEYS + ["title"]:
        if k in assign and assign[k] not in (None, ""):
            old = meta.get(k)
            new = assign[k]
            if old != new:
                meta[k] = new
                changed = True

    new_text = "---\n" + dump_yaml(meta) + "---\n" + body.lstrip()

    if changed and new_text != text:
        md_path.write_text(new_text, "utf-8")
        return True, new_text
    return False, text

# -------------------------------
# Matching: FULL/HALF width insensitive + category-aware scoring
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
            "slug": slugify_title(title),
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
        # strong equal or containment match on normalized forms
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
    ap.add_argument("--out", required=False, default="content_with_meta.zip")
    ap.add_argument("--content-root", default="content", help="content dir inside zip (default: content)")
    ap.add_argument("--dry-run", action="store_true", help="Analyze matches and show report without writing files or zips")
    ap.add_argument("--dedupe", choices=["first","last","none"], default="first", help="De-duplicate CSV rows by (title, category_code). Default: first")
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
        rows = dedupe_rows(rows0)
    elif args.dedupe == "last":
        rows = dedupe_rows_last(rows0)
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

        # rank candidates for each md
        md_candidates = {}
        for md in md_files:
            md_candidates[str(md)] = best_candidates_for_md(md, title_items, args.content_root)

        # Greedy one-to-one assignment prioritizing high-confidence matches
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

        # Apply
        ok = 0
        skipped_no_change = 0
        miss = []
        ambiguous = []
        mapping_report = []  # (md, title, category, changed)

        for md_path, sel in assigned.items():
            if not sel:
                miss.append(md_path)
                continue
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

            if args.dry_run:
                # simulate change detection
                changed = True
            else:
                changed, _ = ensure_front_matter(Path(md_path), assign)

            if changed:
                ok += 1
            else:
                skipped_no_change += 1

            mapping_report.append((md_path, sel["title"], sel["category"], changed))

            # mark ambiguous if there were multiple candidates
            if len(md_candidates.get(md_path, [])) > 1:
                ambiguous.append((md_path, [entry[1]["title"] for entry in md_candidates[md_path]]))

        # Re-pack only if not dry-run
        if not args.dry_run:
            with zipfile.ZipFile(outp, "w", compression=zipfile.ZIP_DEFLATED) as zw:
                for p in (td).rglob("*"):
                    if p.is_file():
                        zw.write(p, arcname=str(p.relative_to(td)))

        # Summary
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
        # Emit mapping report CSV to stdout (can be redirected)
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