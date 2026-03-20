# llm-screener

AI-assisted title/abstract screening for systematic reviews — runs locally, works with EndNote and RIS, applies your own inclusion criteria.

```
Stage 1  ingest    — parse RIS / EndNote XML → deduplicated CSV
Stage 2  classify  — send each record to a local Ollama LLM → decisions
Stage 3  export    — split classified CSV → include / uncertain / exclude files
```

---

## Installation

```bash
pip install -e .
```

Requires Python ≥ 3.8 and no third-party dependencies.

### Ollama (required for Stage 2)

[Ollama](https://ollama.com) runs the AI model locally — your data never leaves your machine.

**macOS / Linux:**
```bash
curl -fsSL https://ollama.com/install.sh | sh
```

**Windows (PowerShell):**
```powershell
irm https://ollama.com/install.ps1 | iex
```

Then pull a model:
```bash
ollama pull qwen3.5:0.8b
```

---

## Quick start

### 1. Ingest

Parse one or more `.xml` (EndNote) or `.ris` files, deduplicate by DOI
(first-seen wins; records without a DOI are never deduplicated), and write
a clean CSV.

```bash
screener ingest \
  --input pubmed.ris embase.xml \
  --output records.csv
```

### 2. Classify

Copy and edit the templates in `templates/` before your first run.

| File | Purpose |
|---|---|
| `templates/system_prompt.txt` | Sets the LLM's role. Edit `[YOUR TOPIC HERE]`. |
| `templates/user_prompt.txt` | Per-record prompt. Uses `{title}`, `{abstract}`, `{criteria}`. |
| `templates/criteria.txt` | Your inclusion/exclusion criteria (injected as `{criteria}`). |

```bash
screener classify \
  --input records.csv \
  --output classified.csv \
  --system-prompt templates/system_prompt.txt \
  --user-prompt   templates/user_prompt.txt \
  --criteria-file templates/criteria.txt \
  --model qwen3.5:0.8b
```

**Resume support:** if `classified.csv` already exists and contains rows with a
`decision`, those rows are preserved and skipped. Only unclassified rows are
sent to the LLM. Safe to kill and restart at any time.

#### Classify options

| Flag | Default | Description |
|---|---|---|
| `--model` | `qwen3.5:0.8b` | Ollama model tag |
| `--temperature` | `0.1` | Sampling temperature |
| `--max-tokens` | `512` | Max tokens generated per record |
| `--num-ctx` | `16384` | Model context window |
| `--max-abstract-chars` | `12000` | Truncate abstracts longer than this before sending to the LLM. Truncated records are flagged with `abstract_truncated=yes` in the output. Raise this if your abstracts are very long and your `--num-ctx` is large enough. |
| `--retry` | `3` | Retry attempts on LLM failure |
| `--log-every` | `10` | Progress log frequency |

### 3. Export

Split the classified CSV into separate files by decision group.

```bash
screener export \
  --input classified.csv \
  --output-dir ./results/ \
  --format ris         # or: xml
```

Produces (skipping any empty groups):

```
results/
  classified_include.ris
  classified_uncertain.ris
  classified_exclude.ris
```

---

## Prompt template placeholders

| Placeholder | Available in | Injected by |
|---|---|---|
| `{criteria}` | system prompt, user prompt | `--criteria-file` content |
| `{title}` | user prompt | record's title column |
| `{abstract}` | user prompt | record's abstract column |

Any other `{placeholder}` in the **system prompt** is left unchanged (so you
can write `{topic}` as a reminder to yourself without causing an error).  
Unknown placeholders in the **user prompt** log a warning.

---

## Decision values

The LLM is expected to reply with one of:

| Value | Meaning |
|---|---|
| `include` | Meets all inclusion criteria |
| `exclude_no_relevance` | Clearly unrelated |
| `exclude_low_relevance` | Tangentially related, criteria not met |
| `exclude_review` | Review/meta-analysis, not primary research |
| `uncertain` | Insufficient information to decide |

These are suggestions in the default template — you can use whatever values
suit your review by editing `templates/user_prompt.txt`. The only special
values in Stage 3 are `include` and `uncertain`; everything else maps to the
`exclude` output file.

---

## CSV column reference

**After ingest:**

`record_id`, `source_file`, `ref_type`, `title`, `authors`, `year`, `journal`,
`volume`, `number`, `abstract`, `doi`, `urls`, `keywords`, `publisher`, `isbn`,
`language`

**Added by classify:**

`decision`, `reason`, `confidence`, `llm_model`, `classified_at`, `abstract_truncated`