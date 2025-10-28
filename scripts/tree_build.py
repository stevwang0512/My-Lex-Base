# scripts/tree_build.py — build site/index/tree.json from content/
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "content"
OUT = ROOT / "site" / "index" / "tree.json"

LEVEL_MAP = {
    '1': '法律',
    '2': '行政法规',
    '3': '司法解释',
    '4': '部门规章'
}

def read_text(p: Path) -> str:
    """Reads text from a file, ignoring errors."""
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def parse_front_matter(p: Path) -> dict:
    """
    Parses YAML front matter from a Markdown file.
    It reads all key-value pairs defined in the YAML block.
    """
    text = read_text(p)
    meta = {}
    content_after_yaml = text

    if text.startswith("---"):
        match = re.search(r"^---\s*$.*?^---\s*$", text, flags=re.S | re.M)
        if match:
            yaml_text = match.group(0).strip("---").strip()
            content_after_yaml = text[match.end():]
            for line in yaml_text.splitlines():
                if ':' in line:
                    key, val = line.split(':', 1)
                    key = key.strip()
                    # Clean up value: remove surrounding quotes and whitespace
                    val = val.strip().strip('"').strip("'")
                    meta[key] = val

    # Fallback for title: use first H1 heading if 'title' is missing in YAML
    if 'title' not in meta or not meta.get('title'):
        h1_match = re.search(r"^\s*#{1,6}\s+(.+?)\s*$", content_after_yaml, flags=re.M)
        meta['title'] = h1_match.group(1).strip() if h1_match else p.stem

    return meta

def level_to_name(level_str: str) -> str:
    """Converts level code ('1', '2', etc.) to its corresponding name."""
    return LEVEL_MAP.get(str(level_str).strip(), '其他')

def build_tree_safe(src: Path):
    """
    Builds a sorted directory tree based on Markdown front matter metadata.
    """
    docs_with_meta = []
    print("[info] Start scanning and parsing metadata from .md files...")
    for p in src.rglob('*.md'):
        meta = parse_front_matter(p)
        # Skip files that lack essential metadata for classification
        if not meta.get('category_code') or not meta.get('level'):
            print(f'[warn] Skipping file with missing metadata: {p.relative_to(ROOT)}')
            continue
        
        meta['path'] = p.relative_to(src).as_posix()
        meta['name'] = p.name
        docs_with_meta.append(meta)
    
    print(f"[info] Parsed {len(docs_with_meta)} documents with metadata.")

    # Globally sort all documents based on the specified keys
    docs_with_meta.sort(key=lambda doc: (
        doc.get('category_code', ''),
        doc.get('level', ''),
        doc.get('catalog_key', ''),
        doc.get('sub_anchor', '')
    ))
    print("[info] Documents sorted globally based on metadata.")

    tree = []
    category_nodes = {}  # Tracks L2 nodes: {category_name: node}
    
    for doc in docs_with_meta:
        category_name = doc.get('category_name', '未分类')
        level_name = level_to_name(doc.get('level', ''))

        # L2 Directory: Find or create category node
        if category_name not in category_nodes:
            category_node = {'name': category_name, 'type': 'dir', 'children': []}
            category_nodes[category_name] = category_node
            tree.append(category_node)
        else:
            category_node = category_nodes[category_name]

        # L3 Directory: Find or create level node within the category
        level_node = None
        for child in category_node['children']:
            if child['name'] == level_name:
                level_node = child
                break
        
        if level_node is None:
            level_node = {'name': level_name, 'type': 'dir', 'children': []}
            category_node['children'].append(level_node)

        # L4 File: Create the file node
        file_node = {
            'name': doc['name'],
            'type': 'file',
            'path': 'content/' + doc['path'],
            'title': doc.get('title', doc['name'])
        }
        level_node['children'].append(file_node)

    print("[info] Tree structure built successfully.")
    return tree

def main():
    if not SRC.exists():
        print('[error] content dir not found:', SRC)
        sys.exit(1)
    tree = build_tree_safe(SRC)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(tree, ensure_ascii=False, indent=2), 'utf-8')
    print('[ok] tree.json written ->', OUT)

if __name__ == '__main__':
    main()
