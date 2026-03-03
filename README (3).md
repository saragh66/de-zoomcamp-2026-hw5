# 🚕 DE ZoomCamp 2026 — Homework 5: Data Platforms with Bruin

Solution for **Homework 5** of the [Data Engineering ZoomCamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp).

---

## ✅ Answers

| # | Question | Answer |
|---|----------|--------|
| 1 | Required files/directories | `.bruin.yml` + `pipeline/` with `pipeline.yml` and `assets/` |
| 2 | Best incremental strategy for time-based NYC data | `time_interval` |
| 3 | Override array variable at runtime | `bruin run --var 'taxi_types=["yellow"]'` |
| 4 | Run asset + all downstream | `bruin run --select ingestion.trips+` |
| 5 | Ensure no NULL in pickup_datetime | `name: not_null` |
| 6 | Visualize dependency graph | `bruin lineage` |
| 7 | First-time run flag | `--full-refresh` |

---

## 📁 Project Structure

```
de-zoomcamp-2026-hw5/
├── .bruin.yml                         ← root config + DuckDB connection
├── .gitignore
├── README.md
└── pipeline/
    ├── pipeline.yml                   ← pipeline definition + variables
    └── assets/
        ├── ingestion/
        │   └── trips.py               ← ingest yellow/green parquet from TLC
        ├── transform/
        │   └── trips_cleaned.sql      ← clean + enrich trip data
        └── reports/
            ├── monthly_summary.sql    ← monthly aggregation report
            └── hourly_demand.sql      ← hourly demand by day of week
```

---

## 🚀 Setup & Run

### 1. Install Bruin CLI
```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

### 2. Clone & enter project
```bash
git clone https://github.com/saragh66/de-zoomcamp-2026-hw5.git
cd de-zoomcamp-2026-hw5
```

### 3. Verify pipeline structure
```bash
bruin validate pipeline/
```

### 4. First-time run (creates all tables from scratch)
```bash
bruin run pipeline/ --full-refresh
```

### 5. Run only yellow taxis
```bash
bruin run pipeline/ --var 'taxi_types=["yellow"]'
```

### 6. Run ingestion + all downstream assets
```bash
bruin run --select ingestion.trips+
```

### 7. View lineage graph
```bash
bruin lineage pipeline/
```

---

## 🔑 Key Concepts Demonstrated

| Concept | Where |
|---------|-------|
| `.bruin.yml` root config | `.bruin.yml` |
| `pipeline.yml` with variables | `pipeline/pipeline.yml` |
| `time_interval` strategy | `ingestion/trips.py` |
| `not_null` quality check | `ingestion/trips.py`, `transform/trips_cleaned.sql` |
| `accepted_values` check | `ingestion/trips.py` |
| Asset dependencies (`depends`) | all SQL assets |
| `--full-refresh` flag | run command |
| `--var` override | run command |
| `bruin lineage` | CLI command |

---

## 🗺️ Pipeline Lineage

```
ingestion.trips
      │
      ▼
transform.trips_cleaned
      │
      ├──▶ reports.monthly_summary
      │
      └──▶ reports.hourly_demand
```

---

*Submitted for DE ZoomCamp 2026 — Module 5: Data Platforms with Bruin*
