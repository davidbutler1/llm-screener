"""Stage 2: classify records using a local LLM via Ollama."""
from __future__ import annotations

import csv
import json
import logging
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .ingest import CANONICAL_FIELDS

# Output fields = canonical fields + screening results
CLASSIFY_FIELDS: List[str] = CANONICAL_FIELDS + [
    'decision',
    'reason',
    'confidence',
    'llm_model',
    'classified_at',
]

# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

_DECISION_RE = re.compile(r'^\s*Decision\s*:\s*(.+?)\s*$', re.I | re.M)
_REASON_RE   = re.compile(r'^\s*Reason\s*:\s*(.+?)\s*$',   re.I | re.M)
_CONF_RE     = re.compile(r'^\s*Confidence\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*$', re.I | re.M)

# Tolerant one-liner: decision | reason | confidence (all labels optional)
_PIPE_RE = re.compile(
    r'^\s*(?:decision\s*:\s*)?(\S+)'
    r'\s*\|\s*(?:reason\s*:\s*)?(.+?)'
    r'(?:\s*\|\s*(?:confidence\s*:\s*)?([0-9]+(?:\.[0-9]+)?))?'
    r'\s*$',
    re.I | re.M,
)


def _parse_response(text: str) -> Tuple[str, str, float]:
    """
    Return (decision, reason, confidence) parsed from LLM output.

    Tries labelled 3-line format first, then tolerant one-liner.
    decision is lowercased; confidence is clamped to [0, 1].
    """
    # Strip markdown noise
    text = text.replace('**', '').replace('`', '')
    text = re.sub(r'^[\-\*\u2022]\s*', '', text, flags=re.M)

    m_dec  = _DECISION_RE.search(text)
    m_rea  = _REASON_RE.search(text)
    m_conf = _CONF_RE.search(text)

    decision = m_dec.group(1).strip().lower() if m_dec else ''
    reason   = m_rea.group(1).strip()         if m_rea  else ''
    conf_str = m_conf.group(1).strip()        if m_conf else ''

    if not decision:
        m = _PIPE_RE.search(text)
        if m:
            decision = m.group(1).strip().lower()
            reason   = (m.group(2) or '').strip()
            conf_str = (m.group(3) or '').strip()

    conf = 0.0
    if conf_str:
        try:
            v = float(conf_str)
            # Values >= 2 are almost certainly percentages (e.g. 90 → 0.90).
            # Values in (1, 2) are out-of-range 0–1 floats; just clamp them.
            conf = v / 100.0 if v >= 2.0 else v
            conf = max(0.0, min(1.0, conf))
        except ValueError:
            pass

    return decision, reason, conf


# ---------------------------------------------------------------------------
# Ollama client
# ---------------------------------------------------------------------------

def _call_ollama(
    model: str,
    system: str,
    prompt: str,
    temperature: float,
    max_tokens: int,
    num_ctx: int,
) -> str:
    """
    POST to the local Ollama API and return the model's response text.
    Raises RuntimeError on HTTP errors, connection failures, or empty replies.
    """
    payload = {
        'model':   model,
        'system':  system,
        'prompt':  prompt,
        'options': {
            'temperature': temperature,
            'num_predict': max_tokens,
            'num_ctx':     num_ctx,
        },
        'stream': False,
    }
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        'http://localhost:11434/api/generate',
        data=data,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )

    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            obj = json.loads(resp.read().decode('utf-8', errors='ignore'))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode('utf-8', errors='ignore')
        raise RuntimeError(f'Ollama HTTP {exc.code}: {body}') from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f'Ollama not reachable: {exc}') from exc

    text = (obj.get('response') or '').strip()
    if not text:
        raise RuntimeError('EMPTY_RESPONSE')
    return text


# ---------------------------------------------------------------------------
# Resume helpers
# ---------------------------------------------------------------------------

def _load_done_ids(output_csv: Path) -> Tuple[Set[str], List[Dict[str, str]]]:
    """
    Read an existing output CSV and return:
      - the set of record_ids that already have a decision
      - all rows (as dicts), keyed so we can re-write them in original order
    """
    done_ids: Set[str] = set()
    rows_by_id: Dict[str, Dict[str, str]] = {}

    if not output_csv.exists():
        return done_ids, []

    with output_csv.open('r', encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rid = row.get('record_id', '')
            rows_by_id[rid] = dict(row)
            if row.get('decision', '').strip():
                done_ids.add(rid)

    return done_ids, list(rows_by_id.values())


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(
    input_csv: Path,
    output_csv: Path,
    system_prompt_file: Path,
    user_prompt_file: Path,
    criteria_file: Optional[Path],
    model: str,
    temperature: float,
    max_tokens: int,
    num_ctx: int,
    retry: int,
    log_every: int,
) -> None:
    """
    Classify every row in input_csv using Ollama and write results to output_csv.

    Resume logic: if output_csv already exists and contains rows with a decision,
    those rows are preserved and skipped; only unclassified rows are sent to the LLM.
    Output preserves the original row order from input_csv.
    """
    # --- Load prompt templates ---
    system_template = system_prompt_file.read_text(encoding='utf-8')
    user_template   = user_prompt_file.read_text(encoding='utf-8')
    criteria        = criteria_file.read_text(encoding='utf-8') if criteria_file else ''

    # System prompt is formatted once (doesn't vary per row).
    # Unknown placeholders (e.g. a literal {topic} the user left in) are kept
    # as-is rather than raising KeyError.
    class _Safe(dict):  # type: ignore[type-arg]
        def __missing__(self, key: str) -> str:
            return '{' + key + '}'

    system_prompt = system_template.format_map(_Safe(criteria=criteria))

    # --- Resume state ---
    done_ids, existing_rows = _load_done_ids(output_csv)
    existing_by_id: Dict[str, Dict[str, str]] = {r['record_id']: r for r in existing_rows}
    if done_ids:
        logging.info('Resuming: %d records already classified, will skip them.', len(done_ids))

    # --- Load input ---
    with input_csv.open('r', encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        input_fieldnames: List[str] = list(reader.fieldnames or CANONICAL_FIELDS)
        all_rows: List[Dict[str, str]] = [dict(r) for r in reader]

    # Determine output field list (canonical + classify columns, no duplicates)
    output_fields: List[str] = list(input_fieldnames)
    for col in ('decision', 'reason', 'confidence', 'llm_model', 'classified_at'):
        if col not in output_fields:
            output_fields.append(col)

    pending = [r for r in all_rows if r.get('record_id', '') not in done_ids]
    logging.info('%d to classify, %d already done.', len(pending), len(done_ids))

    # --- Process ---
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    t_start   = time.time()
    processed = 0

    with output_csv.open('w', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=output_fields, extrasaction='ignore')
        writer.writeheader()

        for row in all_rows:
            rid = row.get('record_id', '')

            # --- Already classified: write as-is ---
            if rid in done_ids:
                writer.writerow(existing_by_id.get(rid, row))
                continue

            # --- Build per-row prompt ---
            title    = row.get('title', '')
            abstract = row.get('abstract', '')
            try:
                user_prompt = user_template.format(
                    title=title,
                    abstract=abstract,
                    criteria=criteria,
                )
            except KeyError as exc:
                logging.warning('User prompt template has unknown placeholder: %s', exc)
                user_prompt = user_template

            # --- Call LLM with retries ---
            decision   = 'uncertain'
            reason     = 'llm_error'
            confidence = 0.0

            for attempt in range(1, retry + 1):
                try:
                    raw = _call_ollama(
                        model, system_prompt, user_prompt,
                        temperature, max_tokens, num_ctx,
                    )
                    decision, reason, confidence = _parse_response(raw)
                    if not decision:
                        snippet = raw.replace('\n', ' ')[:200]
                        logging.warning(
                            'No decision parsed for record_id=%s. Raw: "%s"', rid, snippet
                        )
                        decision = 'uncertain'
                        reason   = 'parse_error'
                    break
                except Exception as exc:
                    logging.warning(
                        'record_id=%s attempt %d/%d failed: %s', rid, attempt, retry, exc
                    )
                    if attempt < retry:
                        time.sleep(1.5 * attempt)
            # If all retries exhausted, defaults remain (uncertain / llm_error / 0.0)

            # --- Annotate row and write ---
            row['decision']      = decision
            row['reason']        = reason
            row['confidence']    = f'{confidence:.3f}'
            row['llm_model']     = model
            row['classified_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

            writer.writerow(row)
            fh.flush()  # ensure partial progress survives interruption
            processed += 1

            if processed % log_every == 0:
                elapsed = time.time() - t_start
                rate    = processed / elapsed if elapsed else 0.0
                pct     = 100.0 * processed / len(pending) if pending else 100.0
                logging.info(
                    'Classified %d/%d (%.1f%%) | %.2f rows/s | elapsed %.1fs',
                    processed, len(pending), pct, rate, elapsed,
                )

    elapsed = time.time() - t_start
    logging.info(
        'Done. Newly classified: %d | Total in output: %d | Elapsed: %.2fs',
        processed, len(all_rows), elapsed,
    )
