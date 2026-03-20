"""Stage 3: split classified CSV into include/uncertain/exclude RIS or XML files."""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Dict, List

from .io import xml_endnote

# Decision values that map to each output group.
# Anything not listed under include/uncertain falls into exclude.
_INCLUDE_VALUES  = {'include'}
_UNCERTAIN_VALUES = {'uncertain'}


def _group(decision: str) -> str:
    d = decision.strip().lower()
    if d in _INCLUDE_VALUES:
        return 'include'
    if d in _UNCERTAIN_VALUES:
        return 'uncertain'
    return 'exclude'


def _write_ris(records: List[Dict[str, str]], path: Path) -> None:
    """Write records as a RIS file."""
    with path.open('w', encoding='utf-8', newline='\n') as fh:
        for rec in records:
            lines: List[str] = []

            # Map ref_type to RIS TY tag (default JOUR)
            _ris_types: Dict[str, str] = {
                'Journal Article': 'JOUR',
                'Book': 'BOOK',
                'Conference Paper': 'CPAPER',
                'Conference Proceedings': 'CONF',
                'Report': 'RPRT',
                'Thesis': 'THES',
                'Web Page': 'ELEC',
            }
            ref_type = rec.get('ref_type', 'Journal Article')
            lines.append(f'TY  - {_ris_types.get(ref_type, "JOUR")}')

            if rec.get('title'):
                lines.append(f'TI  - {rec["title"]}')
            if rec.get('journal'):
                lines.append(f'T2  - {rec["journal"]}')

            for author in (a.strip() for a in (rec.get('authors') or '').split(';') if a.strip()):
                lines.append(f'AU  - {author}')

            if rec.get('year'):
                lines.append(f'PY  - {rec["year"]}')
            if rec.get('volume'):
                lines.append(f'VL  - {rec["volume"]}')
            if rec.get('number'):
                lines.append(f'IS  - {rec["number"]}')
            if rec.get('abstract'):
                lines.append(f'AB  - {rec["abstract"]}')

            for kw in (k.strip() for k in (rec.get('keywords') or '').split(';') if k.strip()):
                lines.append(f'KW  - {kw}')

            doi = rec.get('doi', '')
            if doi:
                lines.append(f'DO  - {doi}')

            for url in (u.strip() for u in (rec.get('urls') or '').split('|') if u.strip()):
                lines.append(f'UR  - {url}')

            if rec.get('publisher'):
                lines.append(f'PB  - {rec["publisher"]}')
            if rec.get('isbn'):
                lines.append(f'SN  - {rec["isbn"]}')
            if rec.get('language'):
                lines.append(f'LA  - {rec["language"]}')

            # Screening metadata in notes
            note_parts: List[str] = []
            for key in ('decision', 'reason', 'confidence', 'llm_model'):
                val = (rec.get(key) or '').strip()
                if val:
                    note_parts.append(f'{key}={val}')
            if note_parts:
                lines.append(f'N1  - {"; ".join(note_parts)}')

            lines.append('ER  -')
            fh.write('\n'.join(lines))
            fh.write('\n\n')


def run(input_csv: Path, output_dir: Path, fmt: str) -> None:
    """
    Read the classified CSV and write one file per decision group:
        <stem>_include.<fmt>
        <stem>_uncertain.<fmt>
        <stem>_exclude.<fmt>

    Groups with zero records are skipped.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = input_csv.stem

    groups: Dict[str, List[Dict[str, str]]] = {
        'include': [],
        'uncertain': [],
        'exclude': [],
    }

    with input_csv.open('r', encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            g = _group(row.get('decision', ''))
            groups[g].append(dict(row))

    total = sum(len(v) for v in groups.values())
    logging.info(
        'Export: %d total → include=%d, uncertain=%d, exclude=%d',
        total,
        len(groups['include']),
        len(groups['uncertain']),
        len(groups['exclude']),
    )

    for group_name, records in groups.items():
        if not records:
            logging.info('Skipping empty group: %s', group_name)
            continue

        out_path = output_dir / f'{stem}_{group_name}.{fmt}'

        if fmt == 'ris':
            _write_ris(records, out_path)
        else:
            xml_endnote.write_xml(records, out_path)

        logging.info('Wrote %d records → %s', len(records), out_path)
