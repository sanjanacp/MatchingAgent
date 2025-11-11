# Matchmaking Agent

AI-powered research agent that pairs SEC Form D / Reg CF deals with Form ADV advisers using a local SQLite lakehouse of parsed filings plus analytics views that compute adviser–deal fit scores.

## Key Capabilities
- Ingests quarterly Form D, Reg CF, and Form ADV datasets into normalized staging tables (`data/staging.sqlite`).
- Materializes feature-rich SQL views (`data/analytics_views.sql`) that clean currency fields, derive ratios/buckets, and build adviser/deal matching candidates.
- Scores every candidate on geography, capital sufficiency, investor audience alignment, ticket fit, traction, and security expertise, exposing both component and composite scores.
- Wraps Google Gemini 2.5 Flash in an Agno agent (`main.py`) with strict planning + SQL guardrails so every response is backed by reproducible queries.
- Emits markdown briefs (see `markdown/`) summarizing top advisers for a deal, including RAUM, component scores, and rationale.

## Repository Layout
```
MatchingAgent/
├── data/
│   ├── staging_schema.sql        # Table definitions for Form D, Reg CF, ADV
│   ├── analytics_views.sql       # Derived views + scoring logic
│   ├── staging.sqlite            # Populated SQLite db (after running loader)
│   └── *.md                      # Research notes (e.g., Reg CF vs Form D overview)
├── tools/
│   ├── load_staging.py           # ETL script for raw TSV/CSV → SQLite
│   └── sequential_thinking_tool.py# Planning scratchpad tool for the agent
├── utils/prompts.py              # System prompt describing workflow & guardrails
├── main.py                       # Entry point that runs the agent and saves markdown output
├── markdown/                     # Generated agent reports
└── README.md                     # This file
```

## Data Pipeline
1. **Raw files**: place quarterly feeds under `~/Downloads/data/` following the expected folder names (e.g., `2025Q1_d`, `2025Q1_cf`, `adv-filing-data-20111105-20241231-part1`).
2. **Run loader**: `python tools/load_staging.py`
   - Drops any existing staging tables defined in `data/staging_schema.sql`.
   - Normalizes booleans/dates, parses currency strings, and writes each TSV/CSV into SQLite.
3. **Materialize views**: `sqlite3 data/staging.sqlite < data/analytics_views.sql`
   - Creates the latest-submission, feature, candidate, and scoring views the agent relies on.

Detailed ETL notes live in `markdown/project_overview.md`, while `markdown/view_scoring_details.md` documents every view and score formula.

## Analytics & Scoring
- `vw_fd_features` / `vw_cf_features` convert issuer economics into clean numerics plus buckets.
- `vw_adv_features` keeps each adviser’s latest RAUM, client mix, affiliations, and registered states.
- `vw_investor_deal_candidates` joins deals to advisers when geography/licensure align and emits binary fit flags.
- `vw_investor_deal_scored` layers continuous component scores and a weighted composite (25% geography, 25% capital, 20% audience, 15% ticket, 10% traction, 5% security).
- See `markdown/view_scoring_details.md` for the full breakdown.

## Agent Workflow
1. **System prompt** (`utils/prompts.py`): enforces plan-first tool usage, schema inspection, SQL-only answers, and markdown outputs containing identifiers, geography, RAUM, component scores, and contact info.
2. **Tools**: the Agno agent loads two tools—`SequentialThinkingTools` (custom planner) and `SQLTools` (runs queries against `data/staging.sqlite`).
3. **Run**: edit `USER_INPUT` in `main.py` or wrap the agent in your own CLI/web interface; when executed, it stores responses under `markdown/output_<timestamp>.md`.

## Getting Started
1. Install dependencies (example):
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt  # create one with agno, pandas, sqlite-utils, etc.
   ```
2. Place raw SEC data in `~/Downloads/data/` as described above.
3. Run `python tools/load_staging.py` to build `data/staging.sqlite`.
4. Apply analytics views: `sqlite3 data/staging.sqlite < data/analytics_views.sql`.
5. Set the Gemini API key if not using the hardcoded placeholder (e.g., via `export GEMINI_API_KEY=...` and update `main.py`).
6. Execute `python main.py` to generate a markdown advisor brief.

## Typical Queries
- “Find the top 5 advisers for Form D accession 0000005108-25-000002.”
- “Show deals that adviser FilingID 1620806 is a fit for.”
- “List Reg CF offerings that allow retail investors but lack Texas-registered advisers.”

Each request triggers: plan → schema check → SQL query (usually against `vw_investor_deal_scored`) → markdown summary & table.

## Extensibility
- Adjust scoring thresholds/weights directly in `data/analytics_views.sql`.
- Add new data feeds or contact tables by extending `staging_schema.sql`, updating `load_staging.py`, and enriching the feature views.
- Swap Gemini for another model supported by Agno by editing the `Agent` constructor in `main.py`.

## Reference Docs
- `markdown/project_overview.md`: narrative of the full pipeline and agent architecture.
- `markdown/view_scoring_details.md`: per-view transformations and score formulas.

---
For questions or enhancements, open an issue or reach out to the data/AI team maintaining this repository.
