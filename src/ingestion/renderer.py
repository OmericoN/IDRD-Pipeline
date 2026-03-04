"""
TEI XML → Markdown extractor.

Extracts:
  - Title, authors, abstract
  - All body sections with inline citations resolved to author-year format
  - All footnotes
  - Full reference list

Output: a single Markdown string (or saved .md file).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from lxml import etree

# TEI namespace — all tags must be prefixed
TEI = "http://www.tei-c.org/ns/1.0"
NS  = {"t": TEI}


# ──────────────────────────────────────────────────────────────────────────────
# Reference parsing
# ──────────────────────────────────────────────────────────────────────────────

def _get_text(el, xpath: str, default: str = "") -> str:
    """Safe single-value xpath text extraction."""
    results = el.xpath(xpath, namespaces=NS)
    if not results:
        return default
    node = results[0]
    return (node.text or "").strip() if hasattr(node, "text") else str(node).strip()


def _parse_references(root: etree._Element) -> Dict[str, dict]:
    """
    Build a dict of  xml:id  →  reference metadata.

    Returns e.g.:
        {
            "b24": {
                "id":      "b24",
                "authors": ["Devlin", "Chang", "Lee", "Toutanova"],
                "year":    "2019",
                "title":   "BERT: Pre-training of Deep Bidirectional ...",
                "venue":   "NAACL",
                "label":   "Devlin et al., 2019",
            },
            ...
        }
    """
    refs: Dict[str, dict] = {}

    for bib in root.xpath("//t:listBibl/t:biblStruct", namespaces=NS):
        xml_id = bib.get("{http://www.w3.org/XML/1998/namespace}id", "")
        if not xml_id:
            continue

        # ── Authors ───────────────────────────────────────────────────
        authors = []
        for persName in bib.xpath(".//t:analytic/t:author/t:persName | .//t:monogr/t:author/t:persName", namespaces=NS):
            surname = _get_text(persName, "t:surname")
            if surname:
                authors.append(surname)

        # ── Year ──────────────────────────────────────────────────────
        year = ""
        for date_el in bib.xpath(".//t:date", namespaces=NS):
            when = date_el.get("when", "")
            if re.match(r"\d{4}", when):
                year = when[:4]
                break
            text = (date_el.text or "").strip()
            m = re.search(r"\d{4}", text)
            if m:
                year = m.group()
                break

        # ── Title ─────────────────────────────────────────────────────
        title = _get_text(bib, ".//t:analytic/t:title[@level='a']")
        if not title:
            title = _get_text(bib, ".//t:monogr/t:title[@level='m']")
        if not title:
            title = _get_text(bib, ".//t:monogr/t:title[@level='j']")

        # ── Venue ─────────────────────────────────────────────────────
        venue = _get_text(bib, ".//t:monogr/t:title[@level='j']")
        if not venue:
            venue = _get_text(bib, ".//t:monogr/t:meeting/t:address/t:settlement")

        # ── Human-readable label ──────────────────────────────────────
        if authors:
            if len(authors) == 1:
                label = f"{authors[0]}, {year}"
            elif len(authors) == 2:
                label = f"{authors[0]} and {authors[1]}, {year}"
            else:
                label = f"{authors[0]} et al., {year}"
        else:
            label = year or xml_id

        # Skip entirely unparseable entries
        if not authors and not title and not year:
            continue

        # Skip entries where the label looks like a raw table/figure caption
        # e.g. "Param", "Resnet", "Icnet", "b59", "b60"
        if not title and (
            not year
            or not authors
            or re.match(r'^b\d+$', xml_id)          # b59, b60, b62 etc.
            or (len(authors) == 1 and not year)      # single token, no year
        ):
            continue

        refs[xml_id] = {
            "id":      xml_id,
            "authors": authors,
            "year":    year,
            "title":   title,
            "venue":   venue,
            "label":   label,
        }

    return refs


def _parse_footnotes(root: etree._Element) -> Dict[str, str]:
    """
    Build a dict of  footnote_id  →  footnote text.
    GROBID puts footnotes as <note place="foot" xml:id="foot_0">.
    """
    notes: Dict[str, str] = {}
    for note in root.xpath("//t:note[@place='foot']", namespaces=NS):
        xml_id = note.get("{http://www.w3.org/XML/1998/namespace}id", "")
        text   = "".join(note.itertext()).strip()
        if xml_id and text:
            notes[xml_id] = text
    return notes


# ──────────────────────────────────────────────────────────────────────────────
# Section filtering
# ──────────────────────────────────────────────────────────────────────────────

# Tags to skip entirely — mathematical/structural noise
SKIP_TAGS = {
    "formula",      # inline and display math
    "figure",       # figures and plots
    "table",        # tables
    "cell",         # table cells
    "row",          # table rows
    "label",        # equation labels
    "graphic",      # images
    "figDesc",      # figure descriptions
    "trash",        # GROBID garbage bin
}

# Sections that are unlikely to contain dataset mentions
EXCLUDED_SECTIONS = {
    "conclusion",
    "conclusions",
    "discussion",
    "related work",
    "related works",
    "acknowledgement",
    "acknowledgements",
    "acknowledgment",
    "acknowledgments",
    "funding",
    "conflict of interest",
    "competing interests",
    "author contributions",
    "ethics statement",
    "broader impact",
    "limitations",
    "supplementary",
    "supplementary material",
    "supplementary materials",
    "supplementary details",
    "appendix",
}


def _is_excluded_section(heading: str) -> bool:
    """Return True if this section heading should be skipped."""
    h = heading.strip().lower()

    if h in EXCLUDED_SECTIONS:
        return True

    excluded_prefixes = (
        "appendix",
        "supplementary",
        "proof",
    )
    for prefix in excluded_prefixes:
        if h.startswith(prefix):
            return True

    # Lettered appendix: "a.", "a.1 ...", "b.", "b.2 ..." etc.
    if re.match(r'^[a-f]\.\s', h):
        return True

    return False


# ──────────────────────────────────────────────────────────────────────────────
# Body text extraction
# ──────────────────────────────────────────────────────────────────────────────

def _element_to_markdown(
    el: etree._Element,
    refs: Dict[str, dict],
    footnotes: Dict[str, str],
    footnote_counters: Dict[str, int],
    used_footnotes: List[Tuple[int, str, str]],
    depth: int = 0,
) -> str:
    tag = etree.QName(el.tag).localname if el.tag != etree.Comment else ""

    # ── Skip noise tags entirely ──────────────────────────────────────
    if tag in SKIP_TAGS:
        return ""

    # ── Section heading ───────────────────────────────────────────────
    if tag == "head":
        n      = el.get("n", "")
        txt    = "".join(el.itertext()).strip()
        prefix = f"{n} " if n else ""
        hashes = "#" * min(depth + 2, 6)
        return f"\n\n{hashes} {prefix}{txt}\n\n"

    # ── Div — skip excluded sections ─────────────────────────────────
    if tag == "div":
        head_el = el.find(f"{{{TEI}}}head")
        if head_el is not None:
            heading_text = "".join(head_el.itertext()).strip()
            if _is_excluded_section(heading_text):
                return ""
        return _children_to_markdown(
            el, refs, footnotes, footnote_counters, used_footnotes, depth + 1
        )

    # ── Inline citation ───────────────────────────────────────────────
    if tag == "ref" and el.get("type") == "bibr":
        target = el.get("target", "").lstrip("#")
        if target and target in refs:
            return f"[{refs[target]['label']}]"
        return f"[{(''.join(el.itertext())).strip()}]"

    # ── Footnote reference ────────────────────────────────────────────
    if tag == "ref" and el.get("type") == "foot":
        target = el.get("target", "").lstrip("#")
        if target in footnotes:
            if target not in footnote_counters:
                n = len(footnote_counters) + 1
                footnote_counters[target] = n
                used_footnotes.append((n, target, footnotes[target]))
            else:
                n = footnote_counters[target]
            return f"[^{n}]"
        return ""

    # ── Paragraph ─────────────────────────────────────────────────────
    if tag == "p":
        inner = _children_to_markdown(
            el, refs, footnotes, footnote_counters, used_footnotes, depth
        )
        text = inner.strip()
        if not text:
            return ""
        return f"\n\n{text}\n\n"

    # ── Default: recurse ──────────────────────────────────────────────
    lead   = (el.text or "").replace("\n", " ")
    result = _children_to_markdown(
        el, refs, footnotes, footnote_counters, used_footnotes, depth
    )
    return lead + result


def _children_to_markdown(
    el: etree._Element,
    refs: Dict[str, dict],
    footnotes: Dict[str, str],
    footnote_counters: Dict[str, int],
    used_footnotes: List[Tuple[int, str, str]],
    depth: int,
) -> str:
    """Convert all children of el, interleaving tail text."""
    parts: List[str] = []

    # Text before the first child
    if el.text:
        parts.append(el.text.replace("\n", " "))

    for child in el:
        # Recurse into child
        parts.append(
            _element_to_markdown(child, refs, footnotes, footnote_counters, used_footnotes, depth)
        )
        # Tail text = text after the closing tag of child, inside parent
        if child.tail:
            parts.append(child.tail.replace("\n", " "))

    return "".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def extract_markdown(xml_path: str | Path) -> str:
    """
    Parse a GROBID TEI XML file and return a Markdown string containing:

      - Title + authors
      - Abstract
      - Body sections relevant to dataset mentions (excludes conclusion,
        discussion, related work, acknowledgements, appendix, etc.)
      - Footnotes as numbered references
      - Full bibliography

    Args:
        xml_path: Path to the .tei.xml file.

    Returns:
        Markdown string.
    """
    tree = etree.parse(str(xml_path))
    root = tree.getroot()

    refs      = _parse_references(root)
    footnotes = _parse_footnotes(root)

    # ── Front matter ──────────────────────────────────────────────────
    title = _get_text(root, "//t:titleStmt/t:title[@type='main']")
    md    = f"# {title}\n\n" if title else ""

    # Authors
    authors = []
    for persName in root.xpath("//t:sourceDesc//t:author/t:persName", namespaces=NS):
        forename = _get_text(persName, "t:forename[@type='first']")
        surname  = _get_text(persName, "t:surname")
        full     = f"{forename} {surname}".strip()
        if full:
            authors.append(full)
    if authors:
        md += f"**Authors:** {', '.join(authors)}\n\n"

    md += "---\n\n"

    # ── Abstract ──────────────────────────────────────────────────────
    abstract_els = root.xpath("//t:abstract//t:p", namespaces=NS)
    if abstract_els:
        md += "## Abstract\n\n"
        for p in abstract_els:
            md += "".join(p.itertext()).strip() + "\n\n"
        md += "---\n\n"

    # ── Body ──────────────────────────────────────────────────────────
    footnote_counters: Dict[str, int]            = {}
    used_footnotes:    List[Tuple[int, str, str]] = []

    body = root.find(f"{{{TEI}}}text/{{{TEI}}}body")
    if body is not None:
        for div in body.xpath("t:div", namespaces=NS):
            md += _element_to_markdown(
                div, refs, footnotes, footnote_counters, used_footnotes, depth=0
            )

    # ── Footnotes ─────────────────────────────────────────────────────
    if used_footnotes:
        md += "\n\n---\n\n## Footnotes\n\n"
        for n, _fid, text in sorted(used_footnotes, key=lambda x: x[0]):
            md += f"[^{n}]: {text}\n\n"

    # ── References ────────────────────────────────────────────────────
    if refs:
        md += "\n\n---\n\n## References\n\n"
        for ref in refs.values():
            authors_str = ", ".join(ref["authors"]) if ref["authors"] else "Unknown"
            venue_str   = f" *{ref['venue']}*." if ref["venue"] else ""
            md += (
                f"- **[{ref['label']}]** "
                f"{authors_str} ({ref['year']}). "
                f"{ref['title']}.{venue_str}\n\n"
            )

    return md


def extract_markdown_to_file(xml_path: str | Path, output_path: str | Path = None) -> Path:
    """
    Extract markdown from a TEI XML file and save it.

    Args:
        xml_path:    Path to .tei.xml
        output_path: Optional output path. Defaults to data/markdown/<stem>.md

    Returns:
        Path to the saved .md file.
    """
    xml_path = Path(xml_path)

    if output_path is None:
        markdown_dir = Path(__file__).parent.parent.parent / "data" / "markdown"
        markdown_dir.mkdir(parents=True, exist_ok=True)
        output_path = markdown_dir / xml_path.with_suffix("").with_suffix("").with_suffix(".md").name
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    md = extract_markdown(xml_path)
    output_path.write_text(md, encoding="utf-8")
    print(f"✓ Extracted markdown → {output_path}")
    return output_path


# ──────────────────────────────────────────────────────────────────────────────
# Standalone
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Default: use the example XML in the repo
        xml = Path(__file__).parent.parent.parent / "data" / "xml"
        files = list(xml.glob("*.tei.xml"))
        if not files:
            print("No .tei.xml files found in data/xml/")
            sys.exit(1)
        target = files[0]
    else:
        target = Path(sys.argv[1])

    out = extract_markdown_to_file(target)
    print(f"\nPreview (first 2000 chars):\n{'='*60}")
    print(out.read_text(encoding="utf-8")[:2000])