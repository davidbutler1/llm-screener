"""CLI entry point: screener ingest | classify | export."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Sub-command handlers
# ---------------------------------------------------------------------------

def _cmd_ingest(args: argparse.Namespace) -> None:
    from .ingest import run
    run(
        inputs=[Path(p) for p in args.input],
        output=Path(args.output),
    )


def _cmd_classify(args: argparse.Namespace) -> None:
    from .classify import run
    run(
        input_csv=Path(args.input),
        output_csv=Path(args.output),
        system_prompt_file=Path(args.system_prompt),
        user_prompt_file=Path(args.user_prompt),
        criteria_file=Path(args.criteria_file) if args.criteria_file else None,
        model=args.model,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        num_ctx=args.num_ctx,
        retry=args.retry,
        log_every=args.log_every,
    )


def _cmd_export(args: argparse.Namespace) -> None:
    from .export import run
    run(
        input_csv=Path(args.input),
        output_dir=Path(args.output_dir),
        fmt=args.format,
    )


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='screener',
        description=(
            'Systematic review screening pipeline.\n\n'
            'Three stages:\n'
            '  ingest    → parse RIS/XML, deduplicate, write CSV\n'
            '  classify  → send each record to a local LLM, write decisions\n'
            '  export    → split classified CSV into include/uncertain/exclude files'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable debug-level logging.',
    )

    sub = parser.add_subparsers(dest='command', required=True)

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------
    p_ingest = sub.add_parser(
        'ingest',
        help='Stage 1: parse .xml/.ris files → deduplicated CSV.',
    )
    p_ingest.add_argument(
        '--input', '-i',
        nargs='+',
        required=True,
        metavar='FILE',
        help='One or more .xml or .ris input files.',
    )
    p_ingest.add_argument(
        '--output', '-o',
        required=True,
        metavar='FILE',
        help='Output CSV path.',
    )

    # ------------------------------------------------------------------
    # classify
    # ------------------------------------------------------------------
    p_classify = sub.add_parser(
        'classify',
        help='Stage 2: classify CSV rows with a local Ollama LLM.',
    )
    p_classify.add_argument(
        '--input', '-i',
        required=True,
        metavar='FILE',
        help='Input CSV produced by "ingest".',
    )
    p_classify.add_argument(
        '--output', '-o',
        required=True,
        metavar='FILE',
        help='Output CSV with added decision/reason/confidence columns.',
    )
    p_classify.add_argument(
        '--system-prompt',
        required=True,
        metavar='FILE',
        help=(
            'Path to the system prompt template file.\n'
            'Available placeholder: {criteria}'
        ),
    )
    p_classify.add_argument(
        '--user-prompt',
        required=True,
        metavar='FILE',
        help=(
            'Path to the user prompt template file.\n'
            'Available placeholders: {title}, {abstract}, {criteria}'
        ),
    )
    p_classify.add_argument(
        '--criteria-file',
        default=None,
        metavar='FILE',
        help=(
            'Optional plain-text file whose contents are injected as {criteria} '
            'into both prompt templates.'
        ),
    )
    p_classify.add_argument(
        '--model',
        default='qwen3.5:0.8b',
        help='Ollama model tag (default: qwen3.5:0.8b).',
    )
    p_classify.add_argument(
        '--temperature',
        type=float,
        default=0.1,
        help='Sampling temperature (default: 0.1).',
    )
    p_classify.add_argument(
        '--max-tokens',
        type=int,
        default=512,
        help='Maximum tokens to generate per record (default: 512).',
    )
    p_classify.add_argument(
        '--num-ctx',
        type=int,
        default=16384,
        help='Model context window in tokens (default: 16384).',
    )
    p_classify.add_argument(
        '--retry',
        type=int,
        default=3,
        help='Number of retry attempts on LLM failure (default: 3).',
    )
    p_classify.add_argument(
        '--log-every',
        type=int,
        default=10,
        help='Log progress every N records (default: 10).',
    )

    # ------------------------------------------------------------------
    # export
    # ------------------------------------------------------------------
    p_export = sub.add_parser(
        'export',
        help='Stage 3: split classified CSV → include/uncertain/exclude files.',
    )
    p_export.add_argument(
        '--input', '-i',
        required=True,
        metavar='FILE',
        help='Classified CSV produced by "classify".',
    )
    p_export.add_argument(
        '--output-dir', '-d',
        required=True,
        metavar='DIR',
        help='Directory where output files are written.',
    )
    p_export.add_argument(
        '--format', '-f',
        choices=['ris', 'xml'],
        default='ris',
        help='Output format (default: ris).',
    )

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        stream=sys.stderr,
    )

    dispatch = {
        'ingest':   _cmd_ingest,
        'classify': _cmd_classify,
        'export':   _cmd_export,
    }

    try:
        dispatch[args.command](args)
    except KeyboardInterrupt:
        logging.warning('Interrupted (Ctrl+C). Partial output may have been written.')
        sys.exit(130)
    except Exception as exc:
        logging.error('%s', exc)
        if args.verbose:
            raise
        sys.exit(1)


if __name__ == '__main__':
    main()
