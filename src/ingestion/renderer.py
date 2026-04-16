""""
TEI XML → Markdown extractor.

Extracts:
  - Title, authors, abstract
  - Body sections (excluding Discussion, Conclusion, Acknowledgement, etc.)
  - Only cited references
  - Footnotes used in body

Output: a single Markdown string (or saved .md file).
"""

from __future__ import annotations

import re
import sys
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from lxml import etree

sys.path.append(str(Path(__file__).parent.parent))

try:
    from models.results import RenderResult
except ImportError:
    # Allow standalone usage without models
    RenderResult = None

# TEI namespace
TEI = "http://www.tei-c.org/ns/1.0"
NS  = {"t": TEI}
logger = logging.getLogger(__name__)

# ── Sections to exclude (case-insensitive, partial match) ─────────────────────
EXCLUDED_SECTION_PATTERNS = [
    r"discuss",
    r"conclusion",
    r"acknowledg",
    r"funding",
    r"conflict.of.interest",
    r"declaration",
    r"supplementar",
    r"appendix",
    r"author.contribution",
    r"abbreviation",
    r"ethical",
]

# Tags whose entire subtree should be silently dropped
SKIP_TAGS = {
    "formula",   # math equations
    "figDesc",   # handled inside figure handler
}


def _is_excluded_section(heading: str) -> bool:
    heading_lower = heading.lower()
    return any(re.search(p, heading_lower) for p in EXCLUDED_SECTION_PATTERNS)


# ──────────────────────────────────────────────────────────────────────────────
# Text cleaning — conservative, only remove known artifacts
# ──────────────────────────────────────────────────────────────────────────────

def _clean_text(text: str) -> str:
    """Remove XML conversion artifacts without damaging real content."""
    if not text:
        return text

    # Remove stray single uppercase letters separated by spaces (e.g. "K I M E T")
    # Must be 3+ such letters to avoid false positives like "A B" abbreviations
    text = re.sub(r'\b([A-Z] ){3,}[A-Z]\b', '', text)

    # Remove stray control characters
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)

    # Collapse multiple spaces
    text = re.sub(r' {2,}', ' ', text).strip()

    return text


def _strip_sup_sub(el: etree._Element) -> str:
    """Extract text from element, skipping all sup/sub children."""
    parts = []
    # leading text of the element itself
    if el.text:
        parts.append(el.text)
    for child in el:
        child_tag = etree.QName(child.tag).localname if child.tag != etree.Comment else ""
        if child_tag in ("sup", "sub", "formula"):
            # skip content but keep tail (text after the tag)
            if child.tail:
                parts.append(child.tail)
        else:
            parts.append(_strip_sup_sub(child))
            if child.tail:
                parts.append(child.tail)
    return "".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Reference & footnote parsing
# ──────────────────────────────────────────────────────────────────────────────

def _get_text(el, xpath: str, default: str = "") -> str:
    results = el.xpath(xpath, namespaces=NS)
    if not results:
        return default
    node = results[0]
    return (node.text or "").strip() if hasattr(node, "text") else str(node).strip()


def _parse_references(root: etree._Element) -> Dict[str, dict]:
    refs: Dict[str, dict] = {}

    for bib in root.xpath("//t:listBibl/t:biblStruct", namespaces=NS):
        xml_id = bib.get("{http://www.w3.org/XML/1998/namespace}id", "")
        if not xml_id:
            continue

        authors = []
        for persName in bib.xpath(
            ".//t:analytic/t:author/t:persName | .//t:monogr/t:author/t:persName",
            namespaces=NS,
        ):
            surname = _get_text(persName, "t:surname")
            if surname:
                authors.append(surname)

        year = ""
        for date_el in bib.xpath(".//t:date", namespaces=NS):
            when = date_el.get("when", "")
            if re.match(r"\d{4}", when):
                year = when[:4]
                break
            m = re.search(r"\d{4}", date_el.text or "")
            if m:
                year = m.group()
                break

        title = _get_text(bib, ".//t:analytic/t:title[@level='a']")
        if not title:
            title = _get_text(bib, ".//t:monogr/t:title[@level='m']")
        if not title:
            title = _get_text(bib, ".//t:monogr/t:title[@level='j']")

        venue = _get_text(bib, ".//t:monogr/t:title[@level='j']")
        if not venue:
            venue = _get_text(bib, ".//t:monogr/t:meeting/t:address/t:settlement")

        doi = ""
        for idno in bib.xpath(".//t:idno[@type='DOI']", namespaces=NS):
            doi = (idno.text or "").strip()
            if doi:
                break

        url = ""
        for ptr in bib.xpath(".//t:ptr[@target]", namespaces=NS):
            url = ptr.get("target", "").strip()
            if url:
                break

        if authors:
            if len(authors) == 1:
                label = f"{authors[0]}, {year}"
            elif len(authors) == 2:
                label = f"{authors[0]} and {authors[1]}, {year}"
            else:
                label = f"{authors[0]} et al., {year}"
        else:
            label = year or xml_id

        refs[xml_id] = {
            "id": xml_id, "authors": authors, "year": year,
            "title": title, "venue": venue, "doi": doi, "url": url, "label": label,
        }

    return refs


def _parse_footnotes(root: etree._Element) -> Dict[str, str]:
    notes: Dict[str, str] = {}
    for note in root.xpath("//t:note[@place='foot']", namespaces=NS):
        xml_id = note.get("{http://www.w3.org/XML/1998/namespace}id", "")
        text   = "".join(note.itertext()).strip()
        if xml_id and text:
            notes[xml_id] = text
    return notes


# ──────────────────────────────────────────────────────────────────────────────
# Body text extraction
# ──────────────────────────────────────────────────────────────────────────────

def _element_to_markdown(
    el: etree._Element,
    refs: Dict[str, dict],
    footnotes: Dict[str, str],
    footnote_counters: Dict[str, int],
    used_footnotes: List[Tuple[int, str, str]],
    cited_refs: Set[str],          # ← track which refs are actually cited
    depth: int = 0,
) -> str:
    if el.tag == etree.Comment:
        return ""

    tag = etree.QName(el.tag).localname

    # ── Silently drop unwanted subtrees ───────────────────────────────
    if tag in SKIP_TAGS:
        return ""

    # ── Superscript / subscript — drop content, keep tail ─────────────
    if tag in ("sup", "sub"):
        return ""

    # ── Section heading ───────────────────────────────────────────────
    if tag == "head":
        txt    = _clean_text(_strip_sup_sub(el))
        n      = el.get("n", "")
        prefix = f"{n} " if n else ""
        hashes = "#" * min(depth + 2, 6)
        return f"\n\n{hashes} {prefix}{txt}\n\n"

    # ── Div — skip excluded sections ──────────────────────────────────
    if tag == "div":
        head_els = el.xpath("t:head", namespaces=NS)
        if head_els:
            heading_text = "".join(head_els[0].itertext()).strip()
            if _is_excluded_section(heading_text):
                return ""
        return _children_to_markdown(
            el, refs, footnotes, footnote_counters, used_footnotes, cited_refs, depth + 1
        )

    # ── Inline citation ───────────────────────────────────────────────
    if tag == "ref" and el.get("type") == "bibr":
        target   = el.get("target", "").lstrip("#")
        original = "".join(el.itertext()).strip()
        if target and target in refs:
            cited_refs.add(target)
            label = refs[target]['label']
            # Keep original marker so downstream can recover placement_content
            if original and original != label:
                return f"[{original}={label}]"
            return f"[{label}]"
        return f"[{original}]" if original else "[?]"

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
            el, refs, footnotes, footnote_counters, used_footnotes, cited_refs, depth
        )
        text = _clean_text(inner)
        return f"\n\n{text}\n\n" if text else ""

    # ── Figure — caption only ─────────────────────────────────────────
    if tag == "figure":
        head_text = ""
        desc      = ""
        for child in el:
            child_tag = etree.QName(child.tag).localname
            if child_tag == "head":
                head_text = _clean_text(_strip_sup_sub(child))
            elif child_tag == "figDesc":
                desc = _clean_text(_strip_sup_sub(child))
        if head_text:
            return f"\n\n**{head_text}**{(': ' + desc) if desc else ''}\n\n"
        return f"\n\n*[Figure: {desc}]*\n\n" if desc else ""

    # ── Table — render as markdown table (first 10 rows) ──────────────
    if tag == "table":
        rows = []
        for i, row in enumerate(el.xpath(".//t:row", namespaces=NS)):
            cells = [_clean_text(_strip_sup_sub(c)) for c in row]
            rows.append("| " + " | ".join(c for c in cells) + " |")
            if i == 0:
                # header separator
                rows.append("| " + " | ".join("---" for _ in cells) + " |")
            if i >= 10:
                rows.append("| ... |")
                break
        return f"\n\n{chr(10).join(rows)}\n\n" if rows else ""

    # ── Default: recurse ──────────────────────────────────────────────
    lead   = (el.text or "").replace("\n", " ")
    result = _children_to_markdown(
        el, refs, footnotes, footnote_counters, used_footnotes, cited_refs, depth
    )
    return lead + result


def _children_to_markdown(
    el: etree._Element,
    refs: Dict[str, dict],
    footnotes: Dict[str, str],
    footnote_counters: Dict[str, int],
    used_footnotes: List[Tuple[int, str, str]],
    cited_refs: Set[str],
    depth: int,
) -> str:
    parts: List[str] = []
    if el.text:
        parts.append(el.text.replace("\n", " "))
    for child in el:
        parts.append(
            _element_to_markdown(
                child, refs, footnotes, footnote_counters, used_footnotes, cited_refs, depth
            )
        )
        if child.tail:
            parts.append(child.tail.replace("\n", " "))
    return "".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def extract_markdown(xml_path: str | Path) -> str:
    tree = etree.parse(str(xml_path))
    root = tree.getroot()

    refs      = _parse_references(root)
    footnotes = _parse_footnotes(root)

    # ── Title ─────────────────────────────────────────────────────────
    title = _get_text(root, "//t:titleStmt/t:title[@type='main']")
    md    = f"# {title}\n\n" if title else ""

    # ── Authors — ONLY forename + surname from persName children ──────
    # We explicitly query forename and surname sub-elements to avoid
    # pulling in affiliation text that sits alongside persName in the tree
    authors = []
    seen_authors: Set[str] = set()
    for persName in root.xpath("//t:sourceDesc//t:author/t:persName", namespaces=NS):
        forename = _get_text(persName, "t:forename[@type='first']")
        if not forename:
            forename = _get_text(persName, "t:forename")
        surname  = _get_text(persName, "t:surname")
        full     = f"{forename} {surname}".strip()
        if full and full not in seen_authors:
            seen_authors.add(full)
            authors.append(full)
    if authors:
        md += f"**Authors:** {', '.join(authors)}\n\n"

    md += "---\n\n"

    # ── Abstract — use _strip_sup_sub to avoid artifacts ──────────────
    abstract_els = root.xpath("//t:abstract//t:p", namespaces=NS)
    if abstract_els:
        md += "## Abstract\n\n"
        for p in abstract_els:
            text = _clean_text(_strip_sup_sub(p))
            if text:
                md += text + "\n\n"
        md += "---\n\n"

    # ── Body ──────────────────────────────────────────────────────────
    footnote_counters: Dict[str, int]            = {}
    used_footnotes:    List[Tuple[int, str, str]] = []
    cited_refs:        Set[str]                   = set()

    body = root.find(f"{{{TEI}}}text/{{{TEI}}}body")
    if body is not None:
        for div in body.xpath("t:div", namespaces=NS):
            md += _element_to_markdown(
                div, refs, footnotes, footnote_counters,
                used_footnotes, cited_refs, depth=0
            )

    # ── Footnotes ─────────────────────────────────────────────────────
    if used_footnotes:
        md += "\n\n---\n\n## Footnotes\n\n"
        for n, _fid, text in sorted(used_footnotes, key=lambda x: x[0]):
            md += f"[^{n}]: {text}\n\n"

    # ── References — ONLY those cited in the remaining body ───────────
    cited_ref_entries = {k: v for k, v in refs.items() if k in cited_refs}
    if cited_ref_entries:
        md += "\n\n---\n\n## References\n\n"
        for ref in cited_ref_entries.values():
            if not ref["title"] and not ref["authors"]:
                continue
            authors_str = ", ".join(ref["authors"]) if ref["authors"] else "Unknown"
            venue_str   = f" *{ref['venue']}*." if ref["venue"] else ""
            doi_str     = (
                f" DOI: [{ref['doi']}](https://doi.org/{ref['doi']})"
                if ref["doi"] else ""
            )
            url_str = f" URL: {ref['url']}" if ref["url"] and not ref["doi"] else ""
            md += (
                f"- **[{ref['label']}]** "
                f"{authors_str} ({ref['year']}). "
                f"{ref['title']}.{venue_str}{doi_str}{url_str}\n\n"
            )

    return md


def extract_markdown_to_file(xml_path: str | Path, output_path: str | Path = None) -> Path:
    xml_path = Path(xml_path)

    if output_path is None:
        markdown_dir = Path(__file__).parent.parent.parent / "data" / "markdown"
        markdown_dir.mkdir(parents=True, exist_ok=True)
        output_path = markdown_dir / xml_path.with_suffix("").with_suffix("").with_suffix(".md").name

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    md = extract_markdown(xml_path)
    output_path.write_text(md, encoding="utf-8")
    logger.info("Extracted markdown -> %s", output_path)
    return output_path


# ──────────────────────────────────────────────────────────────────────────────
# NEW API: Results-based rendering (database-agnostic)
# ──────────────────────────────────────────────────────────────────────────────

def render_to_markdown(
    xml_path: Path,
    output_path: Path = None,
    paper_id: str = None,
    overwrite: bool = False
) -> 'RenderResult':
    """
    Render a single TEI XML file to Markdown and return structured result.
    
    Args:
        xml_path: Path to TEI XML file
        output_path: Output markdown path (auto-generated if None)
        paper_id: Paper ID (extracted from filename if None)
        overwrite: Whether to re-render existing files
        
    Returns:
        RenderResult with success status, paths, error info, etc.
    """
    if RenderResult is None:
        raise ImportError("RenderResult not available - install models package")
    
    xml_path = Path(xml_path)
    paper_id = paper_id or xml_path.stem.replace(".tei", "")
    
    if output_path is None:
        markdown_dir = Path(__file__).parent.parent.parent / "data" / "markdown"
        markdown_dir.mkdir(parents=True, exist_ok=True)
        output_path = markdown_dir / f"{paper_id}.md"
    else:
        output_path = Path(output_path)
    
    # Check if already exists
    if output_path.exists() and not overwrite:
        return RenderResult(
            paper_id=paper_id,
            xml_path=xml_path,
            md_path=output_path,
            success=True,
            message=f"Already exists: {output_path.name}"
        )
    
    # Attempt rendering
    try:
        md_content = extract_markdown(xml_path)
        
        # Count some metrics
        sections = len(re.findall(r'^##\s', md_content, re.MULTILINE))
        references = len(re.findall(r'^\- \*\*\[', md_content, re.MULTILINE))
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(md_content, encoding="utf-8")
        
        return RenderResult(
            paper_id=paper_id,
            xml_path=xml_path,
            md_path=output_path,
            success=True,
            message=f"Rendered: {output_path.name}",
            sections_extracted=sections,
            references_count=references
        )
    
    except Exception as e:
        return RenderResult(
            paper_id=paper_id,
            xml_path=xml_path,
            md_path=output_path,
            success=False,
            message=f"Error: {str(e)}",
            error=str(e)
        )


def render_papers(
    papers: List[Dict],
    paper_id_key: str = 'paperId',
    xml_path_key: str = 'xml_path',
    output_dir: Optional[Path] = None,
    overwrite: bool = False
) -> List['RenderResult']:
    """
    Render multiple TEI XML files to Markdown (database-agnostic).
    
    This method works with any data source - DataFrame, JSON, database query result, etc.
    Results can be persisted to database, DataFrame, or any storage backend.
    
    Args:
        papers: List of dictionaries containing paper metadata with XML paths
        paper_id_key: Key for paper ID in dictionary (default: 'paperId')
        xml_path_key: Key for XML file path (default: 'xml_path')
        output_dir: Output directory for markdown files (auto-detected if None)
        overwrite: Whether to re-render existing files (default: False)
        
    Returns:
        List of RenderResult objects with success/failure info
        
    Example:
        >>> papers = [{'paperId': '123', 'xml_path': '/path/to/file.tei.xml'}]
        >>> results = render_papers(papers)
        >>> successful = [r for r in results if r.success]
    """
    if RenderResult is None:
        raise ImportError("RenderResult not available - install models package")
    
    if not papers:
        logger.info("No papers to render")
        return []
    
    if output_dir is None:
        output_dir = Path(__file__).parent.parent.parent / "data" / "markdown"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Rendering %s XML files to Markdown...", len(papers))
    results = []
    
    for paper in papers:
        paper_id = paper.get(paper_id_key)
        xml_path_str = paper.get(xml_path_key)
        
        if not paper_id or not xml_path_str:
            result = RenderResult(
                paper_id=paper_id or 'unknown',
                xml_path=Path(xml_path_str) if xml_path_str else Path('unknown'),
                md_path=Path('unknown'),
                success=False,
                message="Missing paper ID or XML path",
                error="Missing required fields"
            )
            results.append(result)
            continue
        
        xml_path = Path(xml_path_str)
        output_path = output_dir / f"{paper_id}.md"
        
        result = render_to_markdown(xml_path, output_path, paper_id, overwrite)
        results.append(result)
    
    # Summary
    successful = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)
    logger.info("Rendering complete: %s successful, %s failed", successful, failed)
    
    return results


# ──────────────────────────────────────────────────────────────────────────────
# Standalone
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        xml = Path(__file__).parent.parent.parent / "data" / "xml"
        files = list(xml.glob("*.tei.xml"))
        if not files:
            logger.error("No .tei.xml files found in data/xml/")
            sys.exit(1)
        target = files[0]
    else:
        target = Path(sys.argv[1])

    out = extract_markdown_to_file(target)
    logger.info("Preview (first 2000 chars):\n%s", "=" * 60)
    logger.info("%s", out.read_text(encoding="utf-8")[:2000])
