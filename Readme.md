# 🧠 HireSignal — Job Outcome Intelligence System (JOIS)

> **Analytics engineering system that simulates job marketplace behaviour and transforms raw event data into funnel metrics, churn insights, ghosting analysis, and product intelligence dashboards.**

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit)](https://streamlit.io/)
[![SQLite](https://img.shields.io/badge/SQLite-Database-lightgrey?logo=sqlite)](https://www.sqlite.org/)
[![Plotly](https://img.shields.io/badge/Plotly-Interactive%20Charts-blueviolet?logo=plotly)](https://plotly.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📌 Problem Statement

Job marketplaces suffer from three silent killers: **recruiter ghosting**, **user churn**, and **poor job relevance**. These problems compound — a ghosted applicant loses trust, goes inactive, and churns — yet most platforms have no analytics layer that connects these dots end-to-end.

**HireSignal** is a full analytics engineering system built to answer:
- Where exactly do job-seekers drop out of the hiring funnel?
- Does recruiter ghosting cause measurable churn? By how much?
- Which user segments are most at risk, and why?
- How does job relevance scoring correlate with application rates?

---

## 📊 Business Impact

| Metric | Insight Surfaced |
|---|---|
| **Ghosting → Churn multiplier** | Ghosted applicants churn at significantly higher rates than responded applicants |
| **Funnel drop-off pinpointing** | Identifies the exact stage losing the most users (e.g. Profile → Job Search) |
| **Cohort retention decay** | Week-over-week retention curves reveal when users disengage most |
| **Relevance gap** | Avg relevance score flags ML ranking opportunities to boost apply rates |
| **Segment-level risk scoring** | Low-engagement users show highest churn + lowest activation |

---

## 🏗️ System Architecture

```
Raw Event Simulation (data_gen/)
        │
        ▼
SQLite Database (jois.db)
        │
        ▼
SQL Transformation Layer (sql/)
   ├── stg_events          ← Cleaned, typed event log
   ├── user_summary        ← Per-user KPIs (activated, ghosted, churned)
   ├── kpi_summary         ← Platform-level aggregate metrics
   ├── funnel_time_to_convert ← Time between each funnel stage
   ├── cohort_retention    ← Weekly cohort x week_number retention matrix
   └── ghosting_churn_matrix ← Cross-tab: ghosted × churned
        │
        ▼
Pipeline Orchestrator (database/run_pipeline.py)
        │
        ▼
Streamlit Dashboard (app.py)
   ├── 📊 Overview          ← KPI cards + engagement donut + relevance gauge
   ├── 🔻 Funnel Analysis   ← Amplitude-style funnel + drop-off waterfall + TTC distribution
   ├── 🔁 Cohort Retention  ← Heatmap + retention curves
   ├── 👥 Segmentation      ← Grouped bar KPI comparison by engagement tier
   └── 👻 Ghosting & Churn  ← Stacked bar + Sankey pathway diagram
```

---

## 🖥️ Dashboard Screenshots

### 📊 Overview — Platform KPIs
![Overview Dashboard](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Overview.png)
> KPI tiles for total users, activation rate, ghosting rate, and churn rate. Includes engagement distribution donut chart and job relevance gauge.

---

### 🔻 Funnel Analysis — Drop-off & Time-to-Convert
![Funnel Analysis](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Funnel%20Analysis.png)
> Amplitude-style funnel table with colour-coded drop-off rates, true funnel visualisation, and drop-off waterfall chart. Filterable by device and acquisition source.

---

### 🔁 Cohort Retention — Heatmap & Curves
![Cohort Retention](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Cohort%20retention.png)
> Red-to-green heatmap showing retention % across signup cohorts and week numbers. Line chart comparing retention curves for the first 4 cohorts.

---

### 👥 Segmentation — Engagement Tier Analysis
![Segmentation](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Segmentation.png)
> Grouped bar chart comparing activation rate, ghosting rate, and churn rate across Low / Medium / High engagement segments.

---

### 👻 Ghosting & Churn — Pathway Intelligence
![Ghosting and Churn](https://github.com/Mudit-Thakur/Hiresignal-job-outcome-intelligence-system/blob/main/assets/Dashboard-Ghosting%20%26%20Churn%20Intelligence.png)
> Stacked bar chart showing churn rates for ghosted vs non-ghosted applicants. Sankey diagram traces the full pathway: Applied → Ghosted → Churned/Retained.

---

## 🗂️ Project Structure

```
Hiresignal-job-outcome-intelligence-system/
│
├── app.py                        # Streamlit dashboard (799 lines)
├── requirements.txt              # Python dependencies
├── .gitignore
├── LICENSE
│
├── data_gen/                     # Synthetic event data generator
│   └── generate_events.py
│
├── database/                     # Pipeline orchestrator
│   └── run_pipeline.py
│
└── sql/                          # All SQL transformation scripts
    ├── 01_stg_events.sql
    ├── 02_user_summary.sql
    ├── 03_kpi_summary.sql
    ├── 04_funnel_time_to_convert.sql
    ├── 05_cohort_retention.sql
    └── 06_ghosting_churn_matrix.sql
```

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

### 2. Create a virtual environment (recommended)
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

### 5. Run the data pipeline
Once the dashboard opens in your browser, click **⚡ Run Full Pipeline** in the left sidebar. This will:
- Generate synthetic job marketplace events
- Load them into SQLite
- Run all SQL transformations
- Populate all 5 dashboard pages

---

## 🧪 Key SQL Concepts Used

| Technique | Where Applied |
|---|---|
| CTEs (Common Table Expressions) | Funnel query chains multi-step logic cleanly |
| Window functions | Cohort retention — ranking users within signup week |
| Conditional aggregation | `SUM(CASE WHEN ...)` for funnel stage counts |
| Strict ordered funnel logic | Users counted at step N only if all prior steps completed in sequence |
| Cross-tab analysis | Ghosting × Churn matrix using GROUP BY on two boolean flags |
| NULLIF / ROUND | Safe division for conversion rate % without divide-by-zero |

---

## 📈 Dashboard Pages — Details

### 📊 Overview
- 4 KPI metric tiles: total users, activation rate, ghosting rate, churn rate
- Engagement distribution donut (High / Medium / Low)
- Job relevance score gauge chart (0–1 scale)
- Business insight callouts with delta indicators

### 🔻 Funnel Analysis
- Filterable by **device** and **acquisition source**
- Amplitude-style table with colour-coded drop-off severity
- True funnel chart (Plotly) with hover showing users lost at each stage
- Drop-off waterfall bar chart
- Time-to-convert (TTC) averages + distribution histogram per transition

### 🔁 Cohort Retention
- Red-to-green heatmap: cohort week × week number
- Line chart: retention curves for first 4 cohorts
- Full raw data table (expandable)

### 👥 Segmentation
- Grouped bar: activation %, ghosting %, churn % by engagement tier
- Segment detail table: avg views, applies, relevance score

### 👻 Ghosting & Churn
- Stacked bar: churned vs retained split for ghosted / non-ghosted applicants
- Sankey diagram: full user pathway from "All Applicants" through ghosting to final outcome
- Quantified ghosting-to-churn amplification effect
- Intervention recommendations with measurable expected impact

---

## 🔧 Tech Stack

| Layer | Technology |
|---|---|
| Data generation | Python (Faker, random, datetime) |
| Storage | SQLite via sqlite3 |
| Transformation | Raw SQL (6 layered scripts) |
| Orchestration | Python pipeline runner |
| Visualisation | Plotly (Funnel, Bar, Heatmap, Sankey, Gauge, Pie) |
| Dashboard | Streamlit |
| Data manipulation | Pandas |

---

## 💡 Business Recommendations Surfaced

1. **Recruiter SLA enforcement** — Auto-send closure emails at 72h and 7-day marks to reduce ghosting-driven churn
2. **Week-1 activation nudges** — Cohort retention drops steepest in Week 1; targeted push notifications yield highest ROI
3. **Low-engagement onboarding** — Low-segment users have highest churn; personalised job recommendations can move them to medium
4. **Relevance model improvement** — Avg relevance score below threshold signals ML ranking opportunity to increase apply rates
5. **Recruiter transparency feature** — Surface recruiter response rate scores to job-seekers to improve application decision quality

---

## 👤 Author

**Mudit Thakur** — Freelance Data Analyst  
[GitHub](https://github.com/Mudit-Thakur) · [LinkedIn](https://www.linkedin.com/in/mudit-thakur1/) · [🚀 Live Dashboard](https://hiresignal-job-outcome-intelligence-system-mvugxjn2subp6r8yuyy.streamlit.app/)

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
