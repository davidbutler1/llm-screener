"""Read and write EndNote XML files."""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from xml.dom import minidom

_DOI_PREFIX_RE = re.compile(r'^(?:https?://)?(?:dx\.)?doi\.org/', re.IGNORECASE)
_STYLE_ATTRS = {'face': 'normal', 'font': 'default', 'size': '100%'}

# Codes used by EndNote for common reference types
_REF_TYPE_CODES: Dict[str, int] = {
    'Journal Article': 17,
    'Book': 6,
    'Conference Paper': 47,
    'Conference Proceedings': 10,
    'Report': 27,
    'Thesis': 32,
    'Web Page': 12,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _style_text(node: Optional[ET.Element], path: str) -> str:
    """Find path inside node, return the text of its first <style> child."""
    if node is None:
        return ''
    el = node.find(path)
    if el is None:
        return ''
    style = el.find('style')
    text = style.text if (style is not None and style.text) else (el.text or '')
    return ' '.join(text.split())


def _normalize_doi(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ''
    raw = _DOI_PREFIX_RE.sub('', raw)
    return raw if raw.startswith('10.') else ''


def _styled_child(parent: ET.Element, tag: str, text: str) -> ET.Element:
    """Append <tag><style ...>text</style></tag> to parent."""
    child = ET.SubElement(parent, tag)
    style = ET.SubElement(child, 'style', _STYLE_ATTRS)
    style.text = text
    return child


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

def iter_records(path: Path) -> Iterable[Dict[str, str]]:
    """Stream <record> elements and yield canonical dicts."""
    source = path.name
    context = ET.iterparse(str(path), events=('start', 'end'))
    _, root = next(context)

    for event, elem in context:
        if event != 'end' or elem.tag != 'record':
            continue

        ref_type_el = elem.find('ref-type')
        ref_type = ref_type_el.get('name', '') if ref_type_el is not None else ''

        authors: List[str] = []
        for author_el in elem.findall('.//author'):
            style = author_el.find('style')
            text = (style.text if style is not None else author_el.text) or ''
            text = ' '.join(text.split())
            if text:
                authors.append(text)

        urls: List[str] = []
        seen_urls: set = set()
        for url_el in elem.findall('.//urls/related-urls/url'):
            style = url_el.find('style')
            text = (style.text if style is not None else url_el.text) or ''
            text = ' '.join(text.split())
            if text and text not in seen_urls:
                seen_urls.add(text)
                urls.append(text)

        keywords: List[str] = []
        seen_kw: set = set()
        for kw_el in elem.findall('.//keywords/keyword'):
            style = kw_el.find('style')
            text = (style.text if style is not None else kw_el.text) or ''
            text = ' '.join(text.split())
            if text and text not in seen_kw:
                seen_kw.add(text)
                keywords.append(text)

        doi_raw = _style_text(elem, './/electronic-resource-num')
        doi = _normalize_doi(doi_raw)

        yield {
            'source_file': source,
            'ref_type':    ref_type,
            'title':       _style_text(elem, './/title'),
            'authors':     '; '.join(authors),
            'year':        _style_text(elem, './/year'),
            'journal':     _style_text(elem, './/secondary-title'),
            'volume':      _style_text(elem, './/volume'),
            'number':      _style_text(elem, './/number'),
            'abstract':    _style_text(elem, './/abstract'),
            'doi':         doi,
            'urls':        ' | '.join(urls),
            'keywords':    '; '.join(keywords),
            'publisher':   _style_text(elem, './/publisher'),
            'isbn':        _style_text(elem, './/isbn'),
            'language':    _style_text(elem, './/language'),
        }

        elem.clear()
        root.clear()


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def write_xml(records: List[Dict[str, str]], path: Path) -> None:
    """Write a list of record dicts to an EndNote-compatible XML file."""
    xml_root = ET.Element('xml')
    records_el = ET.SubElement(xml_root, 'records')

    for i, rec in enumerate(records, 1):
        rec_el = ET.SubElement(records_el, 'record')

        # Provenance
        source = rec.get('source_file', 'screener')
        db_el = ET.SubElement(rec_el, 'database', {'name': source, 'path': ''})
        db_el.text = source
        ET.SubElement(rec_el, 'rec-number').text = str(i)

        # Reference type
        ref_type = rec.get('ref_type') or 'Journal Article'
        code = _REF_TYPE_CODES.get(ref_type, 17)
        rt_el = ET.SubElement(rec_el, 'ref-type', {'name': ref_type})
        rt_el.text = str(code)

        # Contributors
        contributors = ET.SubElement(rec_el, 'contributors')
        authors_el = ET.SubElement(contributors, 'authors')
        for author in (a.strip() for a in (rec.get('authors') or '').split(';') if a.strip()):
            _styled_child(authors_el, 'author', author)

        # Titles
        titles = ET.SubElement(rec_el, 'titles')
        if rec.get('title'):
            _styled_child(titles, 'title', rec['title'])
        if rec.get('journal'):
            _styled_child(titles, 'secondary-title', rec['journal'])
            periodical = ET.SubElement(rec_el, 'periodical')
            _styled_child(periodical, 'full-title', rec['journal'])

        # Volume / number
        if rec.get('volume'):
            _styled_child(rec_el, 'volume', rec['volume'])
        if rec.get('number'):
            _styled_child(rec_el, 'number', rec['number'])

        # Keywords
        kws = [k.strip() for k in (rec.get('keywords') or '').split(';') if k.strip()]
        if kws:
            kw_parent = ET.SubElement(rec_el, 'keywords')
            for kw in kws:
                _styled_child(kw_parent, 'keyword', kw)

        # Dates
        if rec.get('year'):
            dates = ET.SubElement(rec_el, 'dates')
            _styled_child(dates, 'year', rec['year'])

        # Identifiers
        if rec.get('isbn'):
            _styled_child(rec_el, 'isbn', rec['isbn'])

        doi = rec.get('doi', '')
        if doi:
            _styled_child(rec_el, 'electronic-resource-num', doi)

        # Abstract
        if rec.get('abstract'):
            _styled_child(rec_el, 'abstract', rec['abstract'])

        # URLs (deduplicated; DOI URL appended if not already present)
        all_urls = [u.strip() for u in (rec.get('urls') or '').split('|') if u.strip()]
        doi_url = f'https://doi.org/{doi}' if doi else ''
        if doi_url and doi_url not in all_urls:
            all_urls.append(doi_url)
        if all_urls:
            urls_el = ET.SubElement(rec_el, 'urls')
            related = ET.SubElement(urls_el, 'related-urls')
            for url in all_urls:
                _styled_child(related, 'url', url)

        # Misc
        if rec.get('publisher'):
            _styled_child(rec_el, 'publisher', rec['publisher'])
        if rec.get('language'):
            _styled_child(rec_el, 'language', rec['language'])

        # Screening results in notes
        note_parts: List[str] = []
        for key in ('decision', 'reason', 'confidence', 'llm_model'):
            val = rec.get(key, '').strip()
            if val:
                note_parts.append(f'{key}={val}')
        if note_parts:
            _styled_child(rec_el, 'notes', ' | '.join(note_parts))

    # Serialise with pretty-printing
    raw = ET.tostring(xml_root, encoding='utf-8')
    pretty = minidom.parseString(raw).toprettyxml(indent='  ', encoding='UTF-8')
    path.write_bytes(pretty)
