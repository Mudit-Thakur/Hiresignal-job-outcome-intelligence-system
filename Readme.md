# 🧠 HireSignal — Job Outcome Intelligence System (JOIS)

> **End-to-end analytics engineering system that transforms raw job marketplace events into funnel intelligence, churn attribution, ghosting analysis, and product health dashboards — built to answer the questions product and growth teams actually care about.**

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Live%20Dashboard-red?logo=streamlit)](https://hiresignal-job-outcome-intelligence-system.streamlit.app/)
[![SQLite](https://img.shields.io/badge/SQLite-Database-lightgrey?logo=sqlite)](https://www.sqlite.org/)
[![Plotly](https://img.shields.io/badge/Plotly-Interactive%20Charts-blueviolet?logo=plotly)](https://plotly.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

### 🚀 [Live Dashboard →](https://hiresignal-job-outcome-intelligence-system.streamlit.app/)

---

## 📌 The Business Problem

Job marketplaces have three silent revenue killers operating in plain sight:

- **Recruiter ghosting** erodes applicant trust after every unanswered application
- **Funnel drop-off** bleeds users before they ever reach the apply stage
- **Silent churn** removes users who never formally cancel — they just stop coming back

These three problems compound each other. A ghosted applicant loses trust, disengages, and churns — yet most platforms have no analytics layer connecting these dots end-to-end.

**HireSignal answers four questions no standard dashboard can:**
1. Where exactly do job-seekers drop out — and which device/source drives the worst drop-off?
2. Does recruiter ghosting cause measurable churn? By exactly how many percentage points?
3. Which cohorts are decaying fastest and at which week does the platform lose them?
4. What is the estimated LTV at risk from ghosted and churned users?

---

## 📊 Business Impact

| Metric | Insight Surfaced |
|---|---|
| **Ghosting → Churn amplification** | Ghosted applicants churn at **91.2%** vs **81.4%** for responded users — a **10pp** amplification effect quantified end-to-end |
| **Funnel drop-off** | **Interview Invite** is the single largest drop-off point losing **70%** of users — filterable by device and acquisition source |
| **Cohort retention decay** | Week-1 avg retention **43.8%** decays to **23.7%** by Week-4 — steepest drop identifies highest-ROI intervention window |
| **Platform health score** | Composite score (Activation 35% + Non-Ghosting 30% + Non-Churn 25% + Relevance 10%) gives leadership a single weekly health number |
| **LTV at risk** | **$410,680** in estimated platform LTV sits in churned users — quantifies the revenue case for re-engagement campaigns |
| **Recruiter response rate** | Response rate by engagement segment surfaces which user groups face the worst recruiter silence |
| **AARRR framework** | All five growth stages mapped to live metrics — Acquisition, Activation, Retention, Referral, Revenue proxy |



---

## 🏗️ System Architecture
Raw Event Simulation (data_gen/generate_events.py)
└── 12 weekly cohorts × 180-260 users + 320 churned users
└── Behavioral profiles: got_invite / ghosted_only / neutral
│
▼
SQLite Database (/tmp/jois.db on cloud | jois.db locally)
│
▼
SQL Transformation Pipeline (sql/) — 5 layered scripts
├── 01_stg_events.sql          ← Cleaned, typed, partitioned event log
├── 02_funnel.sql              ← Strict ordered funnel + TTC + dropout profiles
├── 04a_user_summary.sql       ← Per-user KPIs, churn flags, LTV, AARRR, health score
├── 03_cohort.sql              ← Weekly cohort retention + behavioral cohort retention
└── 04b_retention_health.sql   ← Cohort health flags (Strong/Moderate/Weak/Critical)
│
▼
Pipeline Orchestrator (database/run_pipeline.py)
│
▼
Streamlit Dashboard (app.py)
├── 📊 Overview          ← Health score + KPI cards + AARRR + North Star metric + rolling 4W avg
├── 🔻 Funnel Analysis   ← Amplitude-style funnel + waterfall + TTC distribution + dropout profiles
├── 🔁 Cohort Retention  ← Heatmap + curves + behavioral cohort + retention health flags
├── 👥 Segmentation      ← KPI comparison + recruiter response rate by engagement tier
├── 👻 Ghosting & Churn  ← Stacked bar + Sankey pathway + response rate table
└── 💰 LTV & Revenue     ← LTV:CAC by segment + revenue at risk + LTV distribution

---

## 🖥️ Dashboard Screenshots

### 📊 Overview — Platform KPIs & Health Score
![Overview Dashboard](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Overview.png)
> Platform health score, KPI tiles, North Star metric with 4-week rolling average, AARRR framework mapping, engagement distribution, and relevance gauge.

---

### 🔻 Funnel Analysis — Drop-off & Time-to-Convert
![Funnel Analysis](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Funnel%20Analysis.png)
> Amplitude-style funnel table with colour-coded drop-off severity. True funnel chart, drop-off waterfall, TTC histogram, and dropout profiles by device and source. Filterable by device and acquisition source.

---

### 🔁 Cohort Retention — Heatmap, Curves & Health Flags
![Cohort Retention](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Cohort%20retention.png)
> Red-to-green heatmap across signup cohorts and week numbers. Retention curves for first 4 cohorts. Behavioral cohort retention split (Activated & Responded vs Ghosted vs Never Applied). Per-cohort health flags with W1→W4 decay.

---

### 👥 Segmentation — Engagement Tier Analysis
![Segmentation](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Segmentation.png)
> Grouped bar comparing activation, ghosting, and churn rates across Low/Medium/High engagement segments. Recruiter response rate column surfaces which segments face the worst recruiter silence.

---

### 👻 Ghosting & Churn — Pathway Intelligence
![Ghosting and Churn](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Ghosting%20%26%20Churn%20Intelligence.png)
> Stacked bar quantifying churn rate split for ghosted vs non-ghosted applicants. Sankey diagram traces full user pathway: Applied → Ghosted/Not Ghosted → Churned/Retained. Recruiter response rate table by segment.

---

## 🗂️ Project Structure
Hiresignal-job-outcome-intelligence-system/
│
├── app.py                          # Streamlit dashboard — 6 pages
├── requirements.txt
├── .gitignore
├── LICENSE
│
├── data_gen/
│   └── generate_events.py          # Behavioral event simulator with outcome-driven return profiles
│
├── database/
│   ├── db.py                       # SQLite connection, WAL mode, pipeline log
│   ├── load_raw.py                 # CSV → SQLite raw loader
│   └── run_pipeline.py             # Pipeline orchestrator
│
├── sql/
│   ├── 01_stg_events.sql           # Cleaned event log with date/week/month partitions
│   ├── 02_funnel.sql               # Strict funnel + TTC + dropout profiles
│   ├── 04a_user_summary.sql        # user_summary, kpi_summary, LTV, AARRR, health score
│   ├── 03_cohort.sql               # cohort_retention + behavioral_cohort_retention
│   └── 04b_retention_health.sql    # Per-cohort health flags + decay metrics
│
└── assets/                         # Dashboard screenshots

---

## ⚙️ Setup & Installation

### Prerequisites
- Python 3.10+
- pip

### 1. Clone the repository
```bash
git clone https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system.git
cd Hiresignal-job-outcome-intelligence-system
```

### 2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Launch the dashboard
```bash
streamlit run app.py
```

### 5. Run the pipeline
Click **⚡ Run Full Pipeline** in the sidebar. This generates ~40,000 events, loads them into SQLite, runs 5 SQL transformation scripts, and populates all 6 dashboard pages. Pipeline completes in under 30 seconds.

---

## 🧪 SQL Engineering Concepts Used

| Technique | Where Applied |
|---|---|
| CTEs (multi-step chaining) | Funnel, user_summary, cohort retention — layered logic without subquery hell |
| Window functions | `LAG()` for WoW growth, `AVG() OVER()` for 4-week rolling average, `PARTITION BY` for ghosting matrix |
| Strict ordered funnel logic | Users counted at step N only if all prior steps completed in sequence |
| Conditional aggregation | `SUM(CASE WHEN ...)` for funnel stage counts and KPI flags |
| Cross-tab analysis | Ghosting × Churn matrix using GROUP BY on two boolean flags |
| JULIANDAY arithmetic | Cohort week assignment, days since active, days since signup |
| NULLIF / ROUND | Safe division for all rate calculations without divide-by-zero crashes |
| Behavioral segmentation | `CASE WHEN` engagement tiers + outcome-based cohort grouping |

---

## 📈 Dashboard Pages

### 📊 Overview
Platform health score (composite 0-100), North Star metric with 4-week rolling average, 4 KPI tiles, AARRR framework with live values, engagement distribution donut, job relevance gauge, business insight callouts.

### 🔻 Funnel Analysis
Filterable by device and acquisition source. Amplitude-style table with colour-coded drop-off severity. True funnel chart, drop-off waterfall, TTC averages across all 5 transitions, TTC histogram per transition, dropout profiles by device and source.

### 🔁 Cohort Retention
Red-to-green heatmap (cohort week × week number). Retention curves for first 4 cohorts. Behavioral cohort retention split by outcome group. Per-cohort health flag table with W1→W4 decay column.

### 👥 Segmentation
Grouped bar: activation %, ghosting %, churn % by engagement tier. Segment detail table with avg views, applies, relevance score, and recruiter response rate.

### 👻 Ghosting & Churn
Stacked bar: churned vs retained for ghosted vs non-ghosted applicants. Sankey pathway diagram. Recruiter response rate by segment. Quantified ghosting-to-churn amplification with intervention recommendations.

### 💰 LTV & Revenue
4 LTV KPI tiles including LTV at risk from ghosted and churned users. LTV:CAC ratio table by segment and source with colour-coded benchmark. LTV distribution box plot by engagement tier. Revenue at risk bar chart.

---

## 🔧 Tech Stack

| Layer | Technology |
|---|---|
| Data generation | Python (Faker, random, datetime) — behavioral outcome profiles |
| Storage | SQLite (local/dev) — architecture supports PostgreSQL/BigQuery for production |
| Transformation | Raw SQL — 5 layered scripts with CTEs, window functions, strict funnel logic |
| Orchestration | Python pipeline runner with run logging and duration tracking |
| Visualisation | Plotly (Funnel, Bar, Heatmap, Sankey, Gauge, Pie, Box, Histogram, Scatter) |
| Dashboard | Streamlit — 6 pages, sidebar navigation, pipeline run log |
| Data manipulation | Pandas |

---

## 💡 Business Recommendations Surfaced

1. **Recruiter SLA enforcement** — Auto-send closure emails at 72h and 7-day marks. Reduces ghosting-driven churn at the single highest-leverage intervention point.
2. **Week-1 activation nudges** — Cohort retention decays steepest in Week 1. Targeted push notifications during the first 7 days yield the highest retention ROI.
3. **Low-engagement re-onboarding** — Low-segment users have the highest churn and lowest activation. Personalised job recommendations can move them to medium segment.
4. **Relevance model improvement** — Avg relevance score below 0.5 signals ML ranking opportunity. Closing the gap to 0.7+ is projected to meaningfully lift apply rates.
5. **Recruiter transparency feature** — Surface per-recruiter response rate scores to job-seekers at the point of application. Improves application quality and reduces ghosting volume.
6. **Re-engagement campaigns** — Churned users represent significant trapped LTV. 30-day inactive + previously ghosted is the highest-signal re-engagement segment.

---

## 🎯 Who This Is Built For

This system mirrors the analytics stack used by product and data teams at job marketplace platforms like LinkedIn, Naukri, and Indeed — where funnel analysis, cohort retention, ghosting attribution, and LTV modeling are core weekly reporting deliverables. Every metric, every table, and every dashboard page maps to a real business question these teams answer every sprint.

---

## 👤 Author

**Mudit Thakur** — Data Analyst | SQL · Python · Streamlit · Analytics Engineering
[GitHub](https://github.com/Mudit-Thakur) · [LinkedIn](https://www.linkedin.com/in/mudit-thakur1/) · [🚀 Live Dashboard](https://hiresignal-job-outcome-intelligence-system.streamlit.app/)

---

## 📄 License

GNU General Public License v3.0 (GPL v3) — see [LICENSE](LICENSE) for details.
