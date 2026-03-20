"""Read RIS files and yield canonical record dicts."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

_TAG_RE = re.compile(r'^([A-Z0-9]{2})  - (.*)$')
_DOI_PREFIX_RE = re.compile(r'^(?:https?://)?(?:dx\.)?doi\.org/', re.IGNORECASE)


def _normalize_doi(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ''
    raw = _DOI_PREFIX_RE.sub('', raw)
    return raw if raw.startswith('10.') else ''


def _first(buf: Dict[str, List[str]], *tags: str) -> str:
    for tag in tags:
        vals = buf.get(tag, [])
        if vals:
            return vals[0].strip()
    return ''


def _join(buf: Dict[str, List[str]], *tags: str, sep: str) -> str:
    items: List[str] = []
    for tag in tags:
        items.extend(v.strip() for v in buf.get(tag, []) if v.strip())
    return sep.join(items)


def _finish_record(buf: Dict[str, List[str]], source_file: str) -> Dict[str, str]:
    year_raw = _first(buf, 'PY', 'Y1')
    m = re.search(r'\b(\d{4})\b', year_raw)
    year = m.group(1) if m else year_raw

    doi = _normalize_doi(_first(buf, 'DO', 'DI'))

    return {
        'source_file': source_file,
        'ref_type':    _first(buf, 'TY'),
        'title':       _first(buf, 'TI', 'T1'),
        'authors':     _join(buf, 'AU', sep='; '),
        'year':        year,
        'journal':     _first(buf, 'T2', 'JO', 'JA'),
        'volume':      _first(buf, 'VL'),
        'number':      _first(buf, 'IS'),
        'abstract':    _first(buf, 'AB'),
        'doi':         doi,
        'urls':        _join(buf, 'UR', sep=' | '),
        'keywords':    _join(buf, 'KW', sep='; '),
        'publisher':   _first(buf, 'PB'),
        'isbn':        _first(buf, 'SN'),
        'language':    _first(buf, 'LA'),
    }


def iter_records(path: Path) -> Iterable[Dict[str, str]]:
    """Yield one dict per RIS record."""
    source = path.name
    buf: Dict[str, List[str]] = {}
    last_tag: Optional[str] = None

    with path.open('r', encoding='utf-8', errors='replace') as fh:
        for raw in fh:
            line = raw.rstrip('\n\r')

            if line.startswith('ER  -'):
                if buf:
                    yield _finish_record(buf, source)
                buf = {}
                last_tag = None
                continue

            m = _TAG_RE.match(line)
            if m:
                tag, val = m.group(1), m.group(2).strip()
                buf.setdefault(tag, []).append(val)
                last_tag = tag
            elif last_tag and line.strip():
                # Continuation line — append to the most recent value
                buf[last_tag][-1] = buf[last_tag][-1] + ' ' + line.strip()

    # File without trailing ER
    if buf:
        yield _finish_record(buf, source)
