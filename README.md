# Spotify Streaming Analytics

**An End-to-End Big Data Management Pipeline for the Music Streaming Industry**

STQD6324 Data Management — Dong Qianqi (p161798) Final Report

This repository documents a complete data management pipeline built on a real Spotify personal streaming export for the K-pop group **Stray Kids (SKZ)**, covering listening activity from **April 2025 to April 2026**. The project simulates the kind of event-level data engineering and analytics workflow used in production at music-streaming companies, moving raw playback logs through ingestion, cleaning, warehousing, verification, and visualization.

---

## 1. Project Overview

| Item | Detail |
|---|---|
| Industry | Music streaming / digital entertainment analytics |
| Dataset | `spotify_skz_streaming_4_25_4_26.csv` (27,005 raw playback events) |
| Period covered | April 2025 – April 2026 |
| Development environment | Apache Zeppelin notebook on a Hortonworks-style sandbox cluster |
| Core question | How does this listener engage with the SKZ catalogue over time, by format, by country, and by track? |

---

## 2. Technology Stack Used in This Project

The entire pipeline was developed and executed inside **Apache Zeppelin**, using a different interpreter for each stage so that every tool is used for the task it is best suited to: shell commands for filesystem operations, Pig for distributed ETL, Hive (via JDBC) for SQL warehousing, Spark for in-memory verification and aggregation, and Python for statistical plotting. The table below maps each tool to its exact role; full source code for every tool is included in the project notebook (`notebook/`) and in the accompanying final report.

| Tool / Interpreter | Zeppelin Directive | Role in the Pipeline |
|---|---|---|
| **HDFS** (Hadoop shell) | `%sh` | Creates the distributed directory structure and stages the raw CSV from local disk into HDFS as the single source of truth. |
| **Apache Pig** | `%sh` (`pig -x mapreduce`) | Distributed ETL: filters noisy/short plays, normalises fields, engineers the pseudo-BPM feature, writes cleaned output back to HDFS. |
| **Apache Hive** | `%jdbc(hive)` | SQL data warehouse layer: builds the partitioned fact table and two summary dimension tables (`dim_time_counts`, `dim_albums`) on top of the Pig output. |
| **Apache Spark** (SQL + PySpark) | `%pyspark` | Cross-layer reconciliation audit (fact vs. dimension totals) and all aggregation queries feeding the eight charts. |
| **Python** (Pandas, Matplotlib, Seaborn) | `%pyspark` (`.toPandas()`) | Converts Spark query results to Pandas and renders the eight publication-quality charts used in the EDA section. |
| **Apache Oozie** | `workflow.xml` | Orchestrates Pig → Hive → Spark as a single automated, fault-tolerant production workflow that can be re-triggered on new data. |

**Pipeline flow:**

```
Local CSV
   │  %sh  (HDFS)
   ▼
HDFS raw zone  ──────────────►  Apache Pig (%sh)  ──────────────►  HDFS cleaned zone
                                  ETL: filter + feature engineer

HDFS cleaned zone ──────────►  Apache Hive (%jdbc(hive))  ──────►  fact_streams_detail
                                  Partitioned warehouse              dim_time_counts
                                                                      dim_albums

Hive warehouse ─────────────►  Apache Spark (%pyspark)  ──────►  Reconciliation audit
                                                                  8 aggregation queries

Spark results ──────────────►  Python / Pandas / Matplotlib / Seaborn  ──►  8 charts (EDA)

All of the above ───────────►  Apache Oozie workflow.xml  ──►  Automated re-runnable pipeline
```

---

## 3. Repository Structure

```
.
├── README.md                          # This file
├── report/
│   └── SKZ_Spotify_Final_Report.docx  # Full written report (dataset rationale, code, insights, recommendations)
├── notebook/
│   └── spotify_skz_pipeline.json      # Exported Apache Zeppelin notebook (all interpreters, runnable end to end)
├── data/
│   └── spotify_skz_streaming_4_25_4_26.csv   # Raw source dataset
├── workflow/
│   ├── workflow.xml                   # Apache Oozie orchestration definition
│   └── scripts/
│       ├── clean_streaming.pig        # Apache Pig ETL script
│       ├── build_warehouse.sql        # Apache Hive DDL/DML script
│       └── audit_and_visualize.py     # Spark audit + Python plotting script
└── charts/
    ├── q1_annual_style_evolution.png
    ├── q2_weekly_bpm_fluctuations.png
    ├── q3_release_country_distribution.png
    ├── q4_track_name_density_matrix.png
    ├── q5_top_hits_characteristics.png
    ├── q6_album_type_dominance.png
    ├── q7_user_retention_duration.png
    └── q8_post_release_decay_model.png
```

---

## 4. Data Cleaning

Cleaning is implemented in **Apache Pig**, executed in MapReduce mode (`%sh`, `pig -x mapreduce`) against the raw file staged on HDFS, so it mirrors a production ETL job rather than ad-hoc notebook cleaning.

Steps performed:

1. **Ingestion** — raw CSV copied from local staging into HDFS (`/user/maria_dev/spotify/src_streaming/`); any pre-existing output paths are force-deleted first to guarantee idempotent re-runs.
2. **Schema binding** — all 16 raw columns are loaded with explicit types (`chararray` / `int` / `long`) so type errors surface immediately.
3. **Noise filtering** — rows where `end_time` equals the literal header string are dropped, and rows with `played_ms <= 30000` (under 30 seconds) are removed as accidental skips rather than genuine listens.
4. **Feature engineering** — a pseudo-BPM column is derived per track from title-string length: `(SIZE(track_name) * 3) % 60 + 80`.
5. **Output** — cleaned rows are written back to HDFS at `/processed_streaming/cleaned_output/`, ready for Hive warehousing.

**Cleaning outcome:**

| Metric | Value |
|---|---|
| Raw records (HDFS source) | 27,005 |
| Missing values across all fields | 0 |
| Exact duplicate rows | 0 |
| Records removed (played_ms ≤ 30s) | 531 (1.97%) |
| Records retained for warehousing | 26,474 (98.03%) |

Full script: [`workflow/scripts/clean_streaming.pig`](workflow/scripts/clean_streaming.pig)

---

## 5. Data Warehousing & Verification

**Apache Hive** (`%jdbc(hive)`) loads the Pig output into an external staging table, then inserts into a year/month-partitioned fact table (`fact_streams_detail`) using dynamic partitioning, and derives two summary dimension tables:

- `dim_time_counts` — daily/hourly listening aggregates
- `dim_albums` — per-track/album metrics, including stream count and estimated completion percentage

**Apache Spark** (`%pyspark`) then runs a cross-layer reconciliation audit, joining the fact table against `dim_time_counts` to confirm that total played-minutes match exactly between the detail and summary layers — a zero-data-loss check performed before any chart is produced.

Full scripts: [`workflow/scripts/build_warehouse.sql`](workflow/scripts/build_warehouse.sql), [`workflow/scripts/audit_and_visualize.py`](workflow/scripts/audit_and_visualize.py)

---

## 6. Data Visualizations (EDA)

Eight charts were produced from the warehoused tables using **PySpark** for aggregation and **Pandas / Matplotlib / Seaborn** (`%pyspark`, `.toPandas()`) for rendering, each addressing one specific analytical question.

| # | Chart | Question Answered |
|---|---|---|
| Q1 | Annual Release Volume & Style Evolution | How has SKZ's release format mix changed year over year? |
| Q2 | Weekly Average BPM Fluctuation | Does this listener's music tempo vary by day of week? |
| Q3 | Global Footprint of Album Release Countries | Which countries do SKZ albums get released in? |
| Q4 | Track Title Streaming Frequency | Which tracks are streamed most often? |
| Q5 | Top Hits: Stream Volume vs. Completion Rate | Are the most-played tracks also the most fully-played? |
| Q6 | Total Streams by Album Type | Which album formats drive the most listening volume? |
| Q7 | Listening Duration / Retention Breakdown | How far into a track does the listener typically get? |
| Q8 | Monthly Streaming Volume Trend | Is listening activity seasonal across the year? |

All chart images are in [`charts/`](charts/); full interpretation of each chart is provided in the final report.

---

## 7. Insights & Explanations

Key data-driven findings (full discussion in the report):

- **Format strategy shift** — the catalogue diversified from 2–3 dominant album formats (2018–2022) to 6+ formats per year by 2025, but listening volume is still concentrated in Mini Album and Studio Album.
- **Retention is not the bottleneck** — 98.2% of qualifying plays (those that pass the 30-second noise filter) run past the 2-minute mark; mid-track drop-off is minimal.
- **Frequency ≠ completion** — the highest-frequency tracks are not always the most fully-played, suggesting two distinct listening modes (background/functional vs. deliberate full listens).
- **Tempo does not segment behaviour** — average BPM is essentially flat across all seven days of the week.
- **Clear seasonality** — streaming peaks in April and troughs in June, consistent with release/comeback-driven listening cycles.
- **Market concentration** — 75% of album releases are South Korea-based, with Japan the only meaningful secondary market (16%).

---

## 8. Recommendations

Each recommendation is tied directly to a specific finding above:

1. **Schedule a June re-engagement push** (new mixtape, remix, or playlist feature) to counter the observed seasonal listening trough.
2. **Prioritise Mini Album and Studio Album content in recommendation slots**, since these formats still capture the majority of listening volume despite catalogue diversification.
3. **Shift personalisation budget toward top-of-funnel discovery** (playlist placement, short-form previews) rather than retention features, since mid-track churn is already minimal (98.2% completion).
4. **Use high-frequency/lower-completion tracks for short-form placements** (intros, ad reads, social clips) and high-frequency/100%-completion tracks for full-length editorial playlists.
5. **Maintain the South Korea-first, Japan-second release cadence**, while testing one additional Japan-exclusive release to validate further growth in that secondary market.
6. **Do not invest in day-of-week tempo-based push notifications** for this audience segment, since the BPM curve is statistically flat across the week.

---

## 9. Conclusion

This project built a complete, reproducible data management pipeline for K-pop streaming analytics: raw CSV ingestion into HDFS, noise-filtering ETL in Apache Pig, partitioned warehousing and dimensional modelling in Apache Hive, cross-layer reconciliation in Apache Spark, and eight purpose-built visual analyses rendered in PySpark/Pandas/Matplotlib, all packaged into an Apache Oozie workflow so the entire pipeline can be re-triggered automatically on new data. Out of 27,005 raw events, the pipeline removed 531 (1.97%) low-confidence plays and verified zero data loss across the fact and dimension layers before any chart was produced. The resulting analysis shows a fan base with extremely high mid-track retention, format-diversified content but volume still anchored in core album types, clear release-driven seasonality, and a concentrated two-market release geography — patterns that only emerge once the raw log has been cleaned, warehoused, and aggregated through the pipeline described above.

---

## 10. How to Reproduce

1. Place `spotify_skz_streaming_4_25_4_26.csv` on the cluster's local filesystem (e.g. `/tmp/`).
2. Open `notebook/spotify_skz_pipeline.json` in Apache Zeppelin.
3. Run paragraphs top to bottom — each paragraph is tagged with its interpreter (`%sh`, `%jdbc(hive)`, `%pyspark`) as documented in Section 2 above.
4. Alternatively, trigger the whole pipeline non-interactively via Apache Oozie using [`workflow/workflow.xml`](workflow/workflow.xml).
5. The full written report with all source code, chart-by-chart interpretation, insights, recommendations, and conclusion is available at [`report/SKZ_Spotify_Final_Report.docx`](report/SKZ_Spotify_Final_Report.docx).

---

## Author

Dong Qianqi (p161798)
