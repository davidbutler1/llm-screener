"""Stage 1: ingest RIS/XML files, deduplicate by DOI, write CSV."""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Dict, List, Set

from .io import ris, xml_endnote

# The canonical column order shared by all three stages.
CANONICAL_FIELDS: List[str] = [
    'record_id',
    'source_file',
    'ref_type',
    'title',
    'authors',
    'year',
    'journal',
    'volume',
    'number',
    'abstract',
    'doi',
    'urls',
    'keywords',
    'publisher',
    'isbn',
    'language',
]


def run(inputs: List[Path], output: Path) -> None:
    """
    Read each input file (auto-detected as RIS or XML), deduplicate by DOI
    (first seen wins; records without a DOI are never deduplicated), assign
    sequential record_ids, and write a CSV.
    """
    output.parent.mkdir(parents=True, exist_ok=True)

    seen_dois: Set[str] = set()
    records: List[Dict[str, str]] = []
    next_id = 1
    total_raw = 0

    for path in inputs:
        suffix = path.suffix.lower()
        if suffix == '.xml':
            reader = xml_endnote.iter_records
        elif suffix == '.ris':
            reader = ris.iter_records
        else:
            logging.warning('Skipping unrecognised format: %s', path.name)
            continue

        logging.info('Reading %s', path.name)
        file_count = 0

        for rec in reader(path):
            total_raw += 1
            file_count += 1
            doi = rec.get('doi', '').strip()

            if doi:
                if doi in seen_dois:
                    logging.debug('Duplicate DOI skipped: %s', doi)
                    continue
                seen_dois.add(doi)

            rec['record_id'] = str(next_id)
            next_id += 1
            records.append(rec)

        logging.info('  → %d records read from %s', file_count, path.name)

    duplicates = total_raw - len(records)
    logging.info(
        'Ingest complete: %d raw, %d duplicates removed, %d written to %s',
        total_raw, duplicates, len(records), output,
    )

    with output.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=CANONICAL_FIELDS,
            extrasaction='ignore',
        )
        writer.writeheader()
        writer.writerows(records)
