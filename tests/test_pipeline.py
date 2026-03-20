"""
Tests for screener.

Run with:  python -m pytest tests/ -v
           python -m pytest tests/ -v -k "not ollama"   # skip LLM tests
"""
from __future__ import annotations

import csv
import json
import textwrap
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

FIXTURES = Path(__file__).parent / 'fixtures'
SAMPLE_RIS = FIXTURES / 'sample.ris'
SAMPLE_XML = FIXTURES / 'sample.xml'


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline='', encoding='utf-8') as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# io.ris
# ---------------------------------------------------------------------------

class TestRisReader:
    def test_reads_all_records(self):
        from screener.io.ris import iter_records
        records = list(iter_records(SAMPLE_RIS))
        # 6 entries in the file (including the duplicate)
        assert len(records) == 6

    def test_doi_normalised(self):
        from screener.io.ris import iter_records
        records = list(iter_records(SAMPLE_RIS))
        # All DOIs should be bare (no https://doi.org/ prefix)
        for rec in records:
            doi = rec.get('doi', '')
            if doi:
                assert doi.startswith('10.'), f'DOI not normalised: {doi}'
                assert 'doi.org' not in doi

    def test_authors_semicolon_separated(self):
        from screener.io.ris import iter_records
        records = list(iter_records(SAMPLE_RIS))
        first = records[0]
        assert 'Smith, John A.' in first['authors']
        assert 'Jones, Mary B.' in first['authors']
        assert ';' in first['authors']

    def test_required_fields_present(self):
        from screener.io.ris import iter_records
        required = {'title', 'abstract', 'doi', 'year', 'journal', 'authors'}
        for rec in iter_records(SAMPLE_RIS):
            assert required <= set(rec.keys())

    def test_source_file_set(self):
        from screener.io.ris import iter_records
        records = list(iter_records(SAMPLE_RIS))
        assert all(r['source_file'] == 'sample.ris' for r in records)


# ---------------------------------------------------------------------------
# io.xml_endnote (reader)
# ---------------------------------------------------------------------------

class TestXmlReader:
    def test_reads_two_records(self):
        from screener.io.xml_endnote import iter_records
        records = list(iter_records(SAMPLE_XML))
        assert len(records) == 2

    def test_doi_normalised(self):
        from screener.io.xml_endnote import iter_records
        for rec in iter_records(SAMPLE_XML):
            doi = rec.get('doi', '')
            if doi:
                assert doi.startswith('10.')
                assert 'doi.org' not in doi

    def test_keywords_extracted(self):
        from screener.io.xml_endnote import iter_records
        records = list(iter_records(SAMPLE_XML))
        kws = records[0]['keywords']
        assert 'fibrinogen' in kws
        assert 'traumatic brain injury' in kws

    def test_authors_extracted(self):
        from screener.io.xml_endnote import iter_records
        records = list(iter_records(SAMPLE_XML))
        assert 'Patel, Rohan K.' in records[0]['authors']
        assert 'Larsson, Erik M.' in records[0]['authors']


# ---------------------------------------------------------------------------
# ingest  (Stage 1)
# ---------------------------------------------------------------------------

class TestIngest:
    def test_deduplication_by_doi(self, tmp_path):
        """
        RIS has 6 records (including 1 self-duplicate).
        XML has 2 records (one of which shares a DOI with RIS record 1).
        Expected unique records = 6 - 1 (RIS self-dupe) - 1 (XML/RIS cross-dupe) = 6
        Breakdown: 5 unique from RIS + 1 new from XML = 6
        """
        from screener.ingest import run
        out = tmp_path / 'records.csv'
        run(inputs=[SAMPLE_RIS, SAMPLE_XML], output=out)
        rows = _read_csv(out)
        assert len(rows) == 6, f'Expected 6 unique records, got {len(rows)}'

    def test_record_ids_sequential(self, tmp_path):
        from screener.ingest import run
        out = tmp_path / 'records.csv'
        run(inputs=[SAMPLE_RIS, SAMPLE_XML], output=out)
        rows = _read_csv(out)
        ids = [int(r['record_id']) for r in rows]
        assert ids == list(range(1, len(ids) + 1))

    def test_canonical_fields_present(self, tmp_path):
        from screener.ingest import run, CANONICAL_FIELDS
        out = tmp_path / 'records.csv'
        run(inputs=[SAMPLE_RIS, SAMPLE_XML], output=out)
        rows = _read_csv(out)
        assert rows, 'CSV is empty'
        for field in CANONICAL_FIELDS:
            assert field in rows[0], f'Missing field: {field}'

    def test_single_file_ris(self, tmp_path):
        from screener.ingest import run
        out = tmp_path / 'records.csv'
        run(inputs=[SAMPLE_RIS], output=out)
        rows = _read_csv(out)
        # 6 entries − 1 self-duplicate = 5
        assert len(rows) == 5

    def test_single_file_xml(self, tmp_path):
        from screener.ingest import run
        out = tmp_path / 'records.csv'
        run(inputs=[SAMPLE_XML], output=out)
        rows = _read_csv(out)
        assert len(rows) == 2

    def test_no_doi_records_not_deduplicated(self, tmp_path):
        """Records without a DOI should all pass through."""
        from screener.ingest import run
        # Write a tiny RIS with two no-DOI records
        ris = tmp_path / 'nodoi.ris'
        ris.write_text(textwrap.dedent("""\
            TY  - JOUR
            TI  - Paper without DOI alpha
            AU  - Author, A.
            PY  - 2020
            AB  - Abstract alpha.
            ER  -

            TY  - JOUR
            TI  - Paper without DOI beta
            AU  - Author, B.
            PY  - 2021
            AB  - Abstract beta.
            ER  -
        """), encoding='utf-8')
        out = tmp_path / 'records.csv'
        run(inputs=[ris], output=out)
        rows = _read_csv(out)
        assert len(rows) == 2

    def test_output_directory_created(self, tmp_path):
        from screener.ingest import run
        out = tmp_path / 'subdir' / 'records.csv'
        run(inputs=[SAMPLE_RIS], output=out)
        assert out.exists()


# ---------------------------------------------------------------------------
# classify._parse_response
# ---------------------------------------------------------------------------

class TestParseResponse:
    def _parse(self, text):
        from screener.classify import _parse_response
        return _parse_response(text)

    def test_three_line_format(self):
        text = (
            'Decision: include\n'
            'Reason: Meets all criteria.\n'
            'Confidence: 0.9'
        )
        decision, reason, conf = self._parse(text)
        assert decision == 'include'
        assert 'Meets' in reason
        assert abs(conf - 0.9) < 1e-6

    def test_one_line_pipe_format(self):
        text = 'include | Meets all criteria | 0.85'
        decision, reason, conf = self._parse(text)
        assert decision == 'include'
        assert abs(conf - 0.85) < 1e-6

    def test_pipe_format_no_confidence(self):
        text = 'exclude_no_relevance | Unrelated to TBI'
        decision, reason, conf = self._parse(text)
        assert decision == 'exclude_no_relevance'
        assert conf == 0.0

    def test_markdown_noise_stripped(self):
        text = '**Decision:** include\n**Reason:** Meets criteria.\n**Confidence:** 0.8'
        decision, _, _ = self._parse(text)
        assert decision == 'include'

    def test_bullet_noise_stripped(self):
        text = '- Decision: uncertain\n- Reason: Insufficient abstract.\n- Confidence: 0.5'
        decision, _, _ = self._parse(text)
        assert decision == 'uncertain'

    def test_percentage_confidence_normalised(self):
        text = 'Decision: include\nReason: Yes.\nConfidence: 90'
        _, _, conf = self._parse(text)
        assert abs(conf - 0.9) < 1e-6

    def test_confidence_clamped(self):
        text = 'Decision: include\nReason: Yes.\nConfidence: 1.5'
        _, _, conf = self._parse(text)
        assert conf == 1.0

    def test_empty_input(self):
        decision, reason, conf = self._parse('')
        assert decision == ''
        assert conf == 0.0

    def test_case_insensitive_labels(self):
        text = 'DECISION: include\nREASON: Good.\nCONFIDENCE: 0.7'
        decision, _, conf = self._parse(text)
        assert decision == 'include'
        assert abs(conf - 0.7) < 1e-6


# ---------------------------------------------------------------------------
# classify  (Stage 2) — mocked Ollama
# ---------------------------------------------------------------------------

class TestClassify:
    def _make_input_csv(self, tmp_path: Path) -> Path:
        """Write a minimal ingested CSV for classify to consume."""
        from screener.ingest import CANONICAL_FIELDS
        p = tmp_path / 'records.csv'
        rows = [
            {
                'record_id': '1',
                'source_file': 'test.ris',
                'ref_type': 'Journal Article',
                'title': 'Coagulopathy in isolated TBI',
                'authors': 'Smith, J.',
                'year': '2021',
                'journal': 'Neurotrauma',
                'volume': '38',
                'number': '4',
                'abstract': 'A study on coagulopathy in isolated TBI patients.',
                'doi': '10.1234/test.001',
                'urls': '',
                'keywords': 'TBI; coagulopathy',
                'publisher': '',
                'isbn': '',
                'language': 'English',
            },
            {
                'record_id': '2',
                'source_file': 'test.ris',
                'ref_type': 'Journal Article',
                'title': 'Diabetes prevalence in Africa',
                'authors': 'Jones, K.',
                'year': '2020',
                'journal': 'Diabetes Care',
                'volume': '10',
                'number': '1',
                'abstract': 'Prevalence of diabetes in sub-Saharan Africa.',
                'doi': '10.1234/test.002',
                'urls': '',
                'keywords': 'diabetes',
                'publisher': '',
                'isbn': '',
                'language': 'English',
            },
        ]
        with p.open('w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=CANONICAL_FIELDS, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        return p

    def _make_prompts(self, tmp_path: Path):
        sys_f = tmp_path / 'system.txt'
        usr_f = tmp_path / 'user.txt'
        sys_f.write_text('You are a reviewer. Criteria: {criteria}', encoding='utf-8')
        usr_f.write_text('Title: {title}\nAbstract: {abstract}\nDecision:', encoding='utf-8')
        return sys_f, usr_f

    def _mock_ollama_response(self, decision='include', reason='Meets criteria.', conf=0.9):
        fake_body = json.dumps({
            'response': f'Decision: {decision}\nReason: {reason}\nConfidence: {conf}'
        }).encode('utf-8')
        mock_resp = MagicMock()
        mock_resp.read.return_value = fake_body
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    def test_basic_classify(self, tmp_path):
        from screener.classify import run
        inp = self._make_input_csv(tmp_path)
        out = tmp_path / 'classified.csv'
        sys_f, usr_f = self._make_prompts(tmp_path)

        with patch('urllib.request.urlopen', return_value=self._mock_ollama_response()):
            run(inp, out, sys_f, usr_f, None, 'qwen2.5:7b', 0.1, 512, 8192, 3, 1)

        rows = _read_csv(out)
        assert len(rows) == 2
        for row in rows:
            assert row['decision'] == 'include'
            assert row['llm_model'] == 'qwen2.5:7b'
            assert row['classified_at']
            assert float(row['confidence']) == pytest.approx(0.9)

    def test_classify_adds_columns(self, tmp_path):
        from screener.classify import run
        inp = self._make_input_csv(tmp_path)
        out = tmp_path / 'classified.csv'
        sys_f, usr_f = self._make_prompts(tmp_path)

        with patch('urllib.request.urlopen', return_value=self._mock_ollama_response()):
            run(inp, out, sys_f, usr_f, None, 'qwen2.5:7b', 0.1, 512, 8192, 3, 1)

        rows = _read_csv(out)
        assert 'decision' in rows[0]
        assert 'reason' in rows[0]
        assert 'confidence' in rows[0]
        assert 'llm_model' in rows[0]
        assert 'classified_at' in rows[0]

    def test_resume_skips_done_rows(self, tmp_path):
        """If output already has a row with a decision, it should not be re-sent to LLM."""
        from screener.classify import run, CLASSIFY_FIELDS
        inp = self._make_input_csv(tmp_path)
        out = tmp_path / 'classified.csv'
        sys_f, usr_f = self._make_prompts(tmp_path)

        # Pre-populate output with record_id=1 already classified
        with out.open('w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=CLASSIFY_FIELDS, extrasaction='ignore')
            writer.writeheader()
            writer.writerow({
                'record_id': '1',
                'source_file': 'test.ris',
                'title': 'Coagulopathy in isolated TBI',
                'decision': 'include',
                'reason': 'Pre-existing',
                'confidence': '0.95',
                'llm_model': 'old-model',
                'classified_at': '2024-01-01T00:00:00Z',
            })

        call_count = 0

        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            return self._mock_ollama_response('exclude_no_relevance')

        with patch('urllib.request.urlopen', side_effect=mock_urlopen):
            run(inp, out, sys_f, usr_f, None, 'qwen2.5:7b', 0.1, 512, 8192, 3, 1)

        # Only 1 call for record_id=2; record_id=1 skipped
        assert call_count == 1

        rows = _read_csv(out)
        by_id = {r['record_id']: r for r in rows}
        assert by_id['1']['decision'] == 'include'       # preserved
        assert by_id['2']['decision'] == 'exclude_no_relevance'  # newly classified

    def test_llm_failure_writes_uncertain(self, tmp_path):
        from screener.classify import run
        inp = self._make_input_csv(tmp_path)
        out = tmp_path / 'classified.csv'
        sys_f, usr_f = self._make_prompts(tmp_path)

        with patch('urllib.request.urlopen', side_effect=RuntimeError('EMPTY_RESPONSE')):
            run(inp, out, sys_f, usr_f, None, 'qwen2.5:7b', 0.1, 512, 8192, 1, 1)

        rows = _read_csv(out)
        for row in rows:
            assert row['decision'] == 'uncertain'
            assert row['reason'] == 'llm_error'

    def test_criteria_file_injected(self, tmp_path):
        """Verify that {criteria} in the system prompt is replaced with file contents."""
        from screener.classify import run
        inp = self._make_input_csv(tmp_path)
        out = tmp_path / 'classified.csv'
        sys_f = tmp_path / 'system.txt'
        usr_f = tmp_path / 'user.txt'
        crit_f = tmp_path / 'criteria.txt'

        criteria_text = 'Include only papers about isolated TBI.'
        sys_f.write_text('Role: reviewer. {criteria}', encoding='utf-8')
        usr_f.write_text('Title: {title}\nAbstract: {abstract}', encoding='utf-8')
        crit_f.write_text(criteria_text, encoding='utf-8')

        captured_payloads = []

        def mock_urlopen(req, timeout=None):
            captured_payloads.append(json.loads(req.data.decode()))
            return self._mock_ollama_response()

        with patch('urllib.request.urlopen', side_effect=mock_urlopen):
            run(inp, out, sys_f, usr_f, crit_f, 'model', 0.1, 512, 8192, 1, 1)

        assert captured_payloads, 'No calls made'
        system_used = captured_payloads[0]['system']
        assert criteria_text in system_used


# ---------------------------------------------------------------------------
# export  (Stage 3)
# ---------------------------------------------------------------------------

class TestExport:
    def _make_classified_csv(self, tmp_path: Path) -> Path:
        p = tmp_path / 'classified.csv'
        fieldnames = [
            'record_id', 'source_file', 'ref_type', 'title', 'authors', 'year',
            'journal', 'volume', 'number', 'abstract', 'doi', 'urls', 'keywords',
            'publisher', 'isbn', 'language', 'decision', 'reason', 'confidence',
            'llm_model', 'classified_at',
        ]
        rows = [
            {'record_id': '1', 'title': 'Paper A', 'decision': 'include',
             'abstract': 'About TBI.', 'doi': '10.1/a', 'authors': 'Smith, J.',
             'year': '2021', 'journal': 'J Neuro', 'ref_type': 'Journal Article'},
            {'record_id': '2', 'title': 'Paper B', 'decision': 'exclude_no_relevance',
             'abstract': 'About diabetes.', 'doi': '10.1/b', 'authors': 'Jones, K.',
             'year': '2020', 'journal': 'Diab Care', 'ref_type': 'Journal Article'},
            {'record_id': '3', 'title': 'Paper C', 'decision': 'uncertain',
             'abstract': 'Unclear.', 'doi': '10.1/c', 'authors': 'Lee, M.',
             'year': '2022', 'journal': 'Lancet', 'ref_type': 'Journal Article'},
            {'record_id': '4', 'title': 'Paper D', 'decision': 'exclude_review',
             'abstract': 'Review paper.', 'doi': '10.1/d', 'authors': 'Park, Y.',
             'year': '2019', 'journal': 'NEJM', 'ref_type': 'Journal Article'},
        ]
        with p.open('w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for row in rows:
                full = {f: '' for f in fieldnames}
                full.update(row)
                writer.writerow(full)
        return p

    def test_ris_files_created(self, tmp_path):
        from screener.export import run
        inp = self._make_classified_csv(tmp_path)
        out_dir = tmp_path / 'results'
        run(inp, out_dir, 'ris')
        assert (out_dir / 'classified_include.ris').exists()
        assert (out_dir / 'classified_uncertain.ris').exists()
        assert (out_dir / 'classified_exclude.ris').exists()

    def test_xml_files_created(self, tmp_path):
        from screener.export import run
        inp = self._make_classified_csv(tmp_path)
        out_dir = tmp_path / 'results'
        run(inp, out_dir, 'xml')
        assert (out_dir / 'classified_include.xml').exists()
        assert (out_dir / 'classified_uncertain.xml').exists()
        assert (out_dir / 'classified_exclude.xml').exists()

    def test_exclude_variants_land_in_exclude_file(self, tmp_path):
        """exclude_no_relevance and exclude_review both land in _exclude."""
        from screener.export import run
        inp = self._make_classified_csv(tmp_path)
        out_dir = tmp_path / 'results'
        run(inp, out_dir, 'ris')
        content = (out_dir / 'classified_exclude.ris').read_text(encoding='utf-8')
        assert 'Paper B' in content  # exclude_no_relevance
        assert 'Paper D' in content  # exclude_review

    def test_include_ris_content(self, tmp_path):
        from screener.export import run
        inp = self._make_classified_csv(tmp_path)
        out_dir = tmp_path / 'results'
        run(inp, out_dir, 'ris')
        content = (out_dir / 'classified_include.ris').read_text(encoding='utf-8')
        assert 'Paper A' in content
        assert 'TY  - JOUR' in content
        assert 'ER  -' in content
        # Should NOT contain papers from other groups
        assert 'Paper B' not in content
        assert 'Paper C' not in content

    def test_include_xml_valid(self, tmp_path):
        """XML output should be parseable."""
        import xml.etree.ElementTree as ET
        from screener.export import run
        inp = self._make_classified_csv(tmp_path)
        out_dir = tmp_path / 'results'
        run(inp, out_dir, 'xml')
        tree = ET.parse(out_dir / 'classified_include.xml')
        records = tree.findall('.//record')
        assert len(records) == 1

    def test_empty_group_not_written(self, tmp_path):
        """If a group has zero records it should produce no file."""
        from screener.export import run
        # All records are 'include' → no uncertain or exclude files
        p = tmp_path / 'all_include.csv'
        fieldnames = [
            'record_id', 'title', 'decision', 'abstract', 'doi', 'authors',
            'year', 'journal', 'ref_type', 'source_file', 'volume', 'number',
            'urls', 'keywords', 'publisher', 'isbn', 'language',
            'reason', 'confidence', 'llm_model', 'classified_at',
        ]
        with p.open('w', newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction='ignore')
            w.writeheader()
            w.writerow({f: '' for f in fieldnames} | {
                'record_id': '1', 'title': 'Paper A',
                'decision': 'include', 'ref_type': 'Journal Article',
            })

        out_dir = tmp_path / 'results'
        run(p, out_dir, 'ris')

        assert (out_dir / 'all_include_include.ris').exists()
        assert not (out_dir / 'all_include_uncertain.ris').exists()
        assert not (out_dir / 'all_include_exclude.ris').exists()

    def test_output_dir_created(self, tmp_path):
        from screener.export import run
        inp = self._make_classified_csv(tmp_path)
        out_dir = tmp_path / 'deep' / 'nested' / 'results'
        run(inp, out_dir, 'ris')
        assert out_dir.is_dir()


# ---------------------------------------------------------------------------
# xml_endnote writer round-trip
# ---------------------------------------------------------------------------

class TestXmlWriter:
    def test_write_and_read_back(self, tmp_path):
        from screener.io.xml_endnote import write_xml, iter_records
        records = [
            {
                'source_file': 'test.ris',
                'ref_type': 'Journal Article',
                'title': 'Test paper on TBI',
                'authors': 'Smith, J.; Jones, M.',
                'year': '2021',
                'journal': 'Neurotrauma',
                'volume': '38',
                'number': '4',
                'abstract': 'An abstract.',
                'doi': '10.1234/test',
                'urls': 'https://doi.org/10.1234/test',
                'keywords': 'TBI; coagulopathy',
                'publisher': 'Publisher Inc.',
                'isbn': '1234-5678',
                'language': 'English',
                'decision': 'include',
                'reason': 'Meets all criteria.',
                'confidence': '0.9',
                'llm_model': 'qwen',
            }
        ]
        out = tmp_path / 'output.xml'
        write_xml(records, out)

        read_back = list(iter_records(out))
        assert len(read_back) == 1
        r = read_back[0]
        assert r['title'] == 'Test paper on TBI'
        assert 'Smith, J.' in r['authors']
        assert r['doi'] == '10.1234/test'

    def test_xml_is_valid(self, tmp_path):
        import xml.etree.ElementTree as ET
        from screener.io.xml_endnote import write_xml
        out = tmp_path / 'output.xml'
        write_xml([{
            'title': 'Test', 'authors': 'A, B.', 'year': '2020',
            'ref_type': 'Journal Article', 'doi': '10.1/x',
        }], out)
        tree = ET.parse(out)
        assert tree.getroot().tag == 'xml'


# ---------------------------------------------------------------------------
# End-to-end pipeline (mocked classify)
# ---------------------------------------------------------------------------

class TestEndToEnd:
    def test_full_pipeline(self, tmp_path):
        """Ingest → classify (mocked) → export, all three stages wired together."""
        import json
        from unittest.mock import patch, MagicMock
        from screener.ingest import run as ingest
        from screener.classify import run as classify
        from screener.export import run as export

        # Stage 1
        records_csv = tmp_path / 'records.csv'
        ingest(inputs=[SAMPLE_RIS, SAMPLE_XML], output=records_csv)
        rows = _read_csv(records_csv)
        assert len(rows) == 6

        # Stage 2 (mocked LLM)
        classified_csv = tmp_path / 'classified.csv'
        sys_f = tmp_path / 'sys.txt'
        usr_f = tmp_path / 'usr.txt'
        sys_f.write_text('Reviewer. {criteria}', encoding='utf-8')
        usr_f.write_text('Title: {title}\nAbstract: {abstract}', encoding='utf-8')

        call_idx = 0
        decisions = ['include', 'include', 'exclude_review',
                     'exclude_no_relevance', 'uncertain', 'include']

        def mock_urlopen(req, timeout=None):
            nonlocal call_idx
            d = decisions[call_idx % len(decisions)]
            call_idx += 1
            body = json.dumps({
                'response': f'Decision: {d}\nReason: Test.\nConfidence: 0.8'
            }).encode()
            mock_resp = MagicMock()
            mock_resp.read.return_value = body
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            return mock_resp

        with patch('urllib.request.urlopen', side_effect=mock_urlopen):
            classify(records_csv, classified_csv, sys_f, usr_f, None,
                     'model', 0.1, 512, 8192, 1, 100)

        classified = _read_csv(classified_csv)
        assert len(classified) == 6
        assert all(r['decision'] for r in classified)

        # Stage 3
        results_dir = tmp_path / 'results'
        export(classified_csv, results_dir, 'ris')

        include_f = results_dir / 'classified_include.ris'
        exclude_f = results_dir / 'classified_exclude.ris'
        uncertain_f = results_dir / 'classified_uncertain.ris'

        assert include_f.exists()
        assert exclude_f.exists()
        assert uncertain_f.exists()

        inc_content = include_f.read_text(encoding='utf-8')
        exc_content = exclude_f.read_text(encoding='utf-8')
        unc_content = uncertain_f.read_text(encoding='utf-8')

        # Count ER  - markers = number of records
        assert inc_content.count('ER  -') == 3   # include ×3
        assert exc_content.count('ER  -') == 2   # exclude_review + exclude_no_relevance
        assert unc_content.count('ER  -') == 1   # uncertain ×1
