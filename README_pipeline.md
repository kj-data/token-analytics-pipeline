# Token Analytics Pipeline

A real-time ETL pipeline that ingests live streaming tip events from a REST API, deduplicates records, and persists them to a PostgreSQL database on Supabase.

This pipeline is the data backbone for the [Token Analytics Dashboard](https://token-analytics-dashboard.onrender.com).

---

## How It Works

```
Live Streaming API
       │
       ▼
  Long Polling Loop
  (follows nextUrl pagination)
       │
       ▼
  Filter: tips only
  Deduplicate: in-memory set
       │
       ▼
  Batch INSERT → Supabase (PostgreSQL)
```

The pipeline runs continuously as a **background worker on Railway**, listening for new tip events 24/7 without manual intervention.

---

## Key Design Decisions

- **Long polling** — the API returns a `nextUrl` on every response; the pipeline follows this chain indefinitely, waiting up to ~10 seconds per poll for new events.
- **In-memory deduplication** — a Python `set` tracks processed `event_id`s within each session, preventing duplicate inserts without relying on database constraints.
- **Batch inserts** — events are accumulated per poll and committed in a single transaction using `execute_batch`, minimizing database round-trips.
- **Colombia timezone (UTC-5)** — timestamps are stored in local time to match the streamer's schedule and align with historical data.

---

## Tech Stack

| Layer | Tools |
|---|---|
| Language | Python 3.13 |
| API client | requests |
| Database driver | psycopg2 |
| Database | PostgreSQL (Supabase) |
| Deployment | Railway (Background Worker) |
| Config | python-dotenv |

---

## Database Schema

```sql
CREATE TABLE tips_events (
    event_id  TEXT,
    username  TEXT,
    tokens    INTEGER,
    hour      TIMESTAMPTZ,
    fecha     DATE,
    type      TEXT
);
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `DB_HOST` | Supabase database host |
| `DB_PORT` | Database port (default: 5432) |
| `DB_NAME` | Database name |
| `DB_USER` | Database user |
| `DB_PASSWORD` | Database password |
| `EVENTS_URL` | Streaming platform events API URL |

---

## Running Locally

**1. Clone the repo:**
```bash
git clone https://github.com/kj-data/token-analytics-pipeline.git
cd token-analytics-pipeline
```

**2. Install dependencies:**
```bash
pip install -r requirements.txt
```

**3. Create a `.env` file:**
```
DB_HOST=your_host
DB_PORT=5432
DB_NAME=postgres
DB_USER=your_user
DB_PASSWORD=your_password
EVENTS_URL=your_api_url
```

**4. Run the pipeline:**
```bash
python pipeline.py
```

The pipeline will start polling immediately and log each tip received.

---

## Related Repository

Dashboard that visualizes the data collected by this pipeline:
[token-analytics-dashboard](https://github.com/kj-data/token-analytics-dashboard)
