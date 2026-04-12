import os
import sys
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = os.path.dirname(__file__)
sys.path.insert(0, ROOT)

DB_PATH = "/tmp/jois.db" if os.environ.get("HOME") == "/home/appuser" else os.path.join(ROOT, "jois.db")

st.set_page_config(
    page_title="JOIS — Job Outcome Intelligence System",
    page_icon="🧠",
    layout="wide",
)


def db_exists() -> bool:
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("SELECT 1 FROM kpi_summary")
        conn.close()
        return True
    except Exception:
        return False


def query(sql: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df


def run_pipeline_ui():
    from database.run_pipeline import run_pipeline
    with st.spinner("Running full JOIS pipeline…"):
        run_pipeline()
    st.success("Pipeline complete!")
    st.rerun()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/000000/brain.png", width=64)
    st.title("JOIS")
    st.caption("Job Outcome Intelligence System")
    st.divider()

    if st.button("⚡ Run Full Pipeline", use_container_width=True, type="primary"):
        run_pipeline_ui()

    if db_exists():
        try:
            log_df = query("""
                SELECT run_at, events_loaded, duration_seconds, status
                FROM pipeline_log
                ORDER BY run_id DESC
                LIMIT 1
            """)
            if not log_df.empty:
                last = log_df.iloc[0]
                st.divider()
                st.caption("**Last Pipeline Run**")
                st.caption(f"🕒 {last['run_at']} UTC")
                st.caption(f"📦 {int(last['events_loaded']):,} events loaded")
                st.caption(f"⚡ {last['duration_seconds']}s duration")
                status_icon = "✅" if last["status"] == "success" else "❌"
                st.caption(f"{status_icon} {last['status'].title()}")
                full_log = query("""
                    SELECT run_id, run_at, events_loaded, duration_seconds, status
                    FROM pipeline_log ORDER BY run_id DESC LIMIT 10
                """)
                with st.expander("View run history"):
                    st.dataframe(full_log, hide_index=True, use_container_width=True)
        except Exception:
            pass

    st.divider()
    st.markdown("**Navigation**")
    page = st.radio(
        "Go to",
        [
            "📊 Overview",
            "🔻 Funnel Analysis",
            "🔁 Cohort Retention",
            "👥 Segmentation",
            "👻 Ghosting & Churn",
            "💰 LTV & Revenue",
        ],
        label_visibility="collapsed",
    )

if not db_exists():
    st.title("🧠 Job Outcome Intelligence System")
    st.warning("No data found. Click **⚡ Run Full Pipeline** in the sidebar.")
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.title("📊 Platform Overview")
    st.caption("Key performance indicators for the job marketplace")

    # ── North Star Metric ─────────────────────────────────────────────────────
    st.subheader("🌟 North Star Metric — Weekly Active Applicants")
    st.caption(
        "Users who searched AND applied in the same week. "
        "This is the single metric that best captures platform health."
    )

    ns_df = query("SELECT * FROM north_star_metric ORDER BY event_week")

    if not ns_df.empty:
        latest = ns_df.iloc[-1]

        col_ns1, col_ns2, col_ns3 = st.columns(3)
        col_ns1.metric(
            "Latest Week",
            f"{int(latest['weekly_active_applicants']):,}",
            delta=f"{latest['wow_growth_pct']}% WoW" if pd.notna(latest['wow_growth_pct']) else None,
        )
        col_ns2.metric("Peak Week",  f"{int(ns_df['weekly_active_applicants'].max()):,}")
        col_ns3.metric("Avg Weekly", f"{int(ns_df['weekly_active_applicants'].mean()):,}")

        fig_ns = go.Figure(go.Scatter(
            x=ns_df["event_week"],
            y=ns_df["weekly_active_applicants"],
            mode="lines+markers",
            fill="tozeroy",
            line=dict(color="#636EFA", width=2),
            marker=dict(size=5),
            fillcolor="rgba(99,110,250,0.15)",
        ))
        fig_ns.update_layout(
            height=220,
            margin=dict(t=10, b=10, l=10, r=10),
            paper_bgcolor="#0E1117",
            plot_bgcolor="#0E1117",
            font=dict(color="white"),
            xaxis_title="Week",
            yaxis_title="Active Applicants",
        )
        st.plotly_chart(fig_ns, use_container_width=True)

    st.divider()

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    kpi = query("SELECT * FROM kpi_summary").iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Users",     f"{int(kpi['total_users']):,}")
    c2.metric("Activation Rate", f"{kpi['activation_rate_pct']}%")
    c3.metric("Ghosting Rate",   f"{kpi['ghosting_rate_pct']}%",
              delta=f"-{kpi['ghosting_rate_pct']}% impact", delta_color="inverse")
    c4.metric("Churn Rate",      f"{kpi['churn_rate_pct']}%",
              delta=f"-{kpi['churn_rate_pct']}% at risk", delta_color="inverse")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Engagement Distribution")
        eng_df = pd.DataFrame({
            "Segment": ["High (10+ events)", "Medium (5-9)", "Low (<5)"],
            "Users": [
                int(kpi["high_engagement_users"]),
                int(kpi["medium_engagement_users"]),
                int(kpi["low_engagement_users"]),
            ],
        })
        fig = go.Figure(go.Pie(
            labels=eng_df["Segment"],
            values=eng_df["Users"],
            hole=0.5,
            marker_colors=["#00C9A7", "#FFC300", "#FF5733"],
            textinfo="label+percent",
        ))
        fig.update_layout(
            showlegend=False,
            margin=dict(t=10, b=10),
            paper_bgcolor="#0E1117",
            font=dict(color="white"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Job Relevance Score")
        score = float(kpi["avg_relevance_score"]) if kpi["avg_relevance_score"] else 0
        fig2 = go.Figure(go.Indicator(
            mode="gauge+number",
            value=round(score, 3),
            number={"valueformat": ".3f"},
            gauge={
                "axis": {"range": [0, 1]},
                "bar": {"color": "#636EFA"},
                "steps": [
                    {"range": [0, 0.3],   "color": "#FF5733"},
                    {"range": [0.3, 0.6], "color": "#FFC300"},
                    {"range": [0.6, 1.0], "color": "#00C9A7"},
                ],
            },
            title={"text": "Applies ÷ (Views + Irrelevant Shown)"},
        ))
        fig2.update_layout(
            margin=dict(t=30, b=10),
            paper_bgcolor="#0E1117",
            font=dict(color="white"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # ── AARRR Framework ───────────────────────────────────────────────────────
    st.subheader("📐 AARRR Framework Mapping")
    st.caption("How JOIS metrics map to the standard product growth framework")

    try:
        funnel_top    = query("SELECT users_reached FROM funnel_metrics WHERE step_num = 1").iloc[0]["users_reached"]
        funnel_signup = query("SELECT users_reached FROM funnel_metrics WHERE step_num = 2").iloc[0]["users_reached"]
    except Exception:
        funnel_top    = 0
        funnel_signup = 0

    aarrr_df = pd.DataFrame({
        "Stage": [
            "Acquisition", "Activation", "Retention", "Referral", "Revenue Proxy"
        ],
        "JOIS Metric": [
            "Users who reached session_start",
            "Users who completed profile AND applied",
            "Users active beyond Week 1 (inverse of churn)",
            "Not tracked — referral events not modeled",
            "Interview invite rate (proxy for job placement value)",
        ],
        "Value": [
            f"{int(funnel_top):,} users",
            f"{kpi['activation_rate_pct']}%",
            f"{round(100 - float(kpi['churn_rate_pct']), 1)}% retained",
            "— gap",
            f"{round(100 - float(kpi['ghosting_rate_pct']), 1)}% invite rate",
        ],
        "Status": [
            "✅ Tracked",
            "✅ Tracked",
            "✅ Tracked",
            "⚠️ Not Modeled",
            "✅ Proxy Tracked",
        ],
    })
    st.dataframe(aarrr_df, use_container_width=True, hide_index=True)
    st.warning(
        "**Portfolio Upgrade identified**: Referral cannot be tracked in this project. "
        "But in a real system, addding 'referral' as an acquisition source and track "
        "invite-a-friend events will close the AARRR loop completely."
    )

    st.divider()

    # ── Business Insights ─────────────────────────────────────────────────────
    st.subheader("💡 Business Insights")
    ins1, ins2 = st.columns(2)
    with ins1:
        st.info(
            f"**Activation gap**: Only {kpi['activation_rate_pct']}% of users "
            "have completed both profile setup and job application."
        )
        st.warning(
            f"**Ghosting crisis**: {kpi['ghosting_rate_pct']}% of applicants "
            "have received zero recruiter response — which directly erodes platform trust."
        )
    with ins2:
        st.error(
            f"**Churn signal**: {kpi['churn_rate_pct']}% of eligible users "
            "have gone inactive (>30 days). Many churned after their first ghosting experience."
        )
        st.success(
            f"**Relevance opportunity**: Avg score {score:.3f}. "
            "Improving ML ranking and suggesting relevant jobs could significantly boost apply rates."
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — FUNNEL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔻 Funnel Analysis":
    st.title("🔻 Acquisition Funnel")
    st.caption("Strict ordered funnel — users counted only if they completed ALL prior steps in sequence")

    col_f1, col_f2 = st.columns(2)
    all_devices = ["All"] + query(
        "SELECT DISTINCT device FROM user_summary ORDER BY device"
    )["device"].tolist()
    all_sources = ["All"] + query(
        "SELECT DISTINCT source FROM user_summary ORDER BY source"
    )["source"].tolist()

    with col_f1:
        selected_device = st.selectbox("Filter by Device", all_devices)
    with col_f2:
        selected_source = st.selectbox("Filter by Source", all_sources)

    where_clauses = []
    if selected_device != "All":
        where_clauses.append(f"device = '{selected_device}'")
    if selected_source != "All":
        where_clauses.append(f"source = '{selected_source}'")
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    filtered_funnel_sql = f"""
    WITH filtered_users AS (
        SELECT user_id FROM user_summary {where_sql}
    ),
    fb AS (
        SELECT
            e.user_id,
            MIN(CASE WHEN e.event_type = 'session_start'    THEN e.created_at END) AS ts_session_start,
            MIN(CASE WHEN e.event_type = 'signup'           THEN e.created_at END) AS ts_signup,
            MIN(CASE WHEN e.event_type = 'profile_complete' THEN e.created_at END) AS ts_profile_complete,
            MIN(CASE WHEN e.event_type = 'job_search'       THEN e.created_at END) AS ts_job_search,
            MIN(CASE WHEN e.event_type = 'job_apply'        THEN e.created_at END) AS ts_job_apply,
            MIN(CASE WHEN e.event_type = 'interview_invite' THEN e.created_at END) AS ts_interview_invite
        FROM stg_events e
        JOIN filtered_users f ON e.user_id = f.user_id
        GROUP BY e.user_id
    ),
    fo AS (
        SELECT
            user_id,
            CASE WHEN ts_session_start IS NOT NULL THEN 1 ELSE 0 END AS s1,
            CASE WHEN ts_signup > ts_session_start THEN 1 ELSE 0 END AS s2,
            CASE WHEN ts_profile_complete > ts_signup THEN 1 ELSE 0 END AS s3,
            CASE WHEN ts_job_search > ts_profile_complete THEN 1 ELSE 0 END AS s4,
            CASE WHEN ts_job_apply > ts_job_search THEN 1 ELSE 0 END AS s5,
            CASE WHEN ts_interview_invite > ts_job_apply THEN 1 ELSE 0 END AS s6
        FROM fb
    ),
    fs AS (
        SELECT
            SUM(s1) AS n1,
            SUM(CASE WHEN s1=1 AND s2=1 THEN 1 ELSE 0 END) AS n2,
            SUM(CASE WHEN s1=1 AND s2=1 AND s3=1 THEN 1 ELSE 0 END) AS n3,
            SUM(CASE WHEN s1=1 AND s2=1 AND s3=1 AND s4=1 THEN 1 ELSE 0 END) AS n4,
            SUM(CASE WHEN s1=1 AND s2=1 AND s3=1 AND s4=1 AND s5=1 THEN 1 ELSE 0 END) AS n5,
            SUM(CASE WHEN s1=1 AND s2=1 AND s3=1 AND s4=1 AND s5=1 AND s6=1 THEN 1 ELSE 0 END) AS n6
        FROM fo
    )
    SELECT
        step_num, step_name, users_reached,
        prev AS users_at_prev_step,
        ROUND(100.0 * users_reached / NULLIF(prev, 0), 1) AS conversion_rate_pct,
        ROUND(100.0 * (prev - users_reached) / NULLIF(prev, 0), 1) AS drop_off_rate_pct,
        (prev - users_reached) AS users_dropped
    FROM (
        SELECT 1 AS step_num, 'session_start'    AS step_name, n1 AS users_reached, n1 AS prev FROM fs
        UNION ALL SELECT 2, 'signup',           n2, n1 FROM fs
        UNION ALL SELECT 3, 'profile_complete', n3, n2 FROM fs
        UNION ALL SELECT 4, 'job_search',       n4, n3 FROM fs
        UNION ALL SELECT 5, 'job_apply',        n5, n4 FROM fs
        UNION ALL SELECT 6, 'interview_invite', n6, n5 FROM fs
    )
    ORDER BY step_num
    """

    funnel_df = query(filtered_funnel_sql)

    if selected_device != "All" or selected_source != "All":
        active_filters = []
        if selected_device != "All":
            active_filters.append(f"Device: **{selected_device}**")
        if selected_source != "All":
            active_filters.append(f"Source: **{selected_source}**")
        st.info(
            f"Filtered by {' | '.join(active_filters)} — "
            f"showing {int(funnel_df['users_reached'].iloc[0]):,} users"
        )

    # ── Funnel Table ──────────────────────────────────────────────────────────
    st.subheader("📋 Funnel Table (Amplitude-style)")
    display_cols = [
        "step_num", "step_name", "users_reached",
        "conversion_rate_pct", "drop_off_rate_pct", "users_dropped",
    ]
    styled = funnel_df[display_cols].rename(columns={
        "step_num": "Step #", "step_name": "Stage",
        "users_reached": "Users Reached", "conversion_rate_pct": "Conv. Rate %",
        "drop_off_rate_pct": "Drop-off %", "users_dropped": "Users Dropped",
    })

    def color_dropoff(val):
        if pd.isna(val):
            return ""
        if val >= 100:
            return "background-color: #8B0000; color: white"
        if val >= 50:
            return "background-color: #FF6B35; color: white"
        if val >= 25:
            return "background-color: #FFC300; color: black"
        return "background-color: #00C9A7; color: black"

    st.dataframe(
        styled.style.map(color_dropoff, subset=["Drop-off %"]),
        use_container_width=True, hide_index=True,
    )

    st.divider()

    # ── Funnel Chart ──────────────────────────────────────────────────────────
    st.subheader("📐 True Funnel Visualization")

    hover_texts = []
    for i, row in funnel_df.iterrows():
        if row["step_num"] == 1:
            hover_texts.append(
                f"<b>{row['step_name']}</b><br>Users: {int(row['users_reached']):,}<br>(Top of funnel)"
            )
        else:
            hover_texts.append(
                f"<b>{row['step_name']}</b><br>"
                f"Users: {int(row['users_reached']):,}<br>"
                f"Conv. from prev: {row['conversion_rate_pct']}%<br>"
                f"Drop-off: {row['drop_off_rate_pct']}%<br>"
                f"Lost: {int(row['users_dropped']):,} users"
            )

    fig = go.Figure(go.Funnel(
        y=funnel_df["step_name"].tolist(),
        x=funnel_df["users_reached"].tolist(),
        textposition="inside",
        textinfo="value+percent previous",
        hovertext=hover_texts,
        hoverinfo="text",
        marker=dict(
            color=["#4C72B0", "#55A868", "#C44E52", "#8172B2", "#CCB974", "#64B5CD"],
            line=dict(width=2, color="white"),
        ),
        connector=dict(line=dict(color="rgba(255,255,255,0.3)", dash="dot", width=2)),
    ))
    fig.update_layout(
        title={"text": "Job Marketplace Acquisition Funnel", "x": 0.5, "font": {"size": 18}},
        height=520,
        margin=dict(l=20, r=20, t=60, b=20),
        paper_bgcolor="#0E1117",
        plot_bgcolor="#0E1117",
        font=dict(color="white", size=13),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Drop-off Waterfall ────────────────────────────────────────────────────
    st.subheader("📉 Drop-off Waterfall")
    wf_df = funnel_df[funnel_df["step_num"] > 1].copy()
    fig_wf = go.Figure(go.Bar(
        x=wf_df["step_name"],
        y=wf_df["users_dropped"],
        marker_color=wf_df["drop_off_rate_pct"].apply(
            lambda v: "#FF5733" if v >= 40 else ("#FFC300" if v >= 20 else "#00C9A7")
        ),
        text=wf_df["drop_off_rate_pct"].apply(lambda v: f"{v}%"),
        textposition="outside",
    ))
    fig_wf.update_layout(
        title="Users Lost at Each Transition",
        xaxis_title="Funnel Stage",
        yaxis_title="Users Dropped",
        height=380,
        paper_bgcolor="#0E1117",
        plot_bgcolor="#0E1117",
        font=dict(color="white"),
    )
    st.plotly_chart(fig_wf, use_container_width=True)

    worst = funnel_df.iloc[1:]["drop_off_rate_pct"].idxmax()
    worst_row = funnel_df.loc[worst]
    st.error(
        f"🚨 **Biggest drop-off**: **{worst_row['step_name']}** loses "
        f"{worst_row['drop_off_rate_pct']}% of incoming users "
        f"({int(worst_row['users_dropped']):,} people)."
    )

    st.divider()

    # ── Time to Convert ───────────────────────────────────────────────────────
    st.subheader("⏱️ Time-to-Convert Analysis")
    st.caption("Average time users spend between each funnel stage")

    ttc_df = query("""
        SELECT
            ROUND(AVG(CASE WHEN mins_session_to_signup > 0 THEN mins_session_to_signup END), 1) AS avg_session_to_signup,
            ROUND(AVG(CASE WHEN mins_signup_to_profile > 0 THEN mins_signup_to_profile END), 1) AS avg_signup_to_profile,
            ROUND(AVG(CASE WHEN mins_profile_to_search > 0 THEN mins_profile_to_search END), 1) AS avg_profile_to_search,
            ROUND(AVG(CASE WHEN mins_search_to_apply   > 0 THEN mins_search_to_apply   END), 1) AS avg_search_to_apply,
            ROUND(AVG(CASE WHEN days_apply_to_invite   > 0 THEN days_apply_to_invite   END), 1) AS avg_apply_to_invite
        FROM funnel_time_to_convert
    """)

    ttc_row = ttc_df.iloc[0]
    t1, t2, t3, t4, t5 = st.columns(5)
    t1.metric("Session → Signup",  f"{ttc_row['avg_session_to_signup']} min")
    t2.metric("Signup → Profile",  f"{ttc_row['avg_signup_to_profile']} min")
    t3.metric("Profile → Search",  f"{ttc_row['avg_profile_to_search']} min")
    t4.metric("Search → Apply",    f"{ttc_row['avg_search_to_apply']} min")
    t5.metric("Apply → Invite",    f"{ttc_row['avg_apply_to_invite']} days")

    ttc_raw = query("""
        SELECT mins_session_to_signup, mins_signup_to_profile,
               mins_profile_to_search, mins_search_to_apply
        FROM funnel_time_to_convert
        WHERE mins_session_to_signup > 0
    """)

    transition_cols = {
        "mins_session_to_signup":  "Session → Signup (min)",
        "mins_signup_to_profile":  "Signup → Profile (min)",
        "mins_profile_to_search":  "Profile → Search (min)",
        "mins_search_to_apply":    "Search → Apply (min)",
    }

    selected_transition = st.selectbox(
        "Inspect distribution for transition",
        list(transition_cols.keys()),
        format_func=lambda k: transition_cols[k],
    )

    col_data = ttc_raw[selected_transition].dropna()
    col_data = col_data[col_data <= col_data.quantile(0.95)]

    fig_ttc = go.Figure(go.Histogram(
        x=col_data, nbinsx=40, marker_color="#636EFA", opacity=0.85,
    ))
    fig_ttc.update_layout(
        title=f"Distribution: {transition_cols[selected_transition]}",
        xaxis_title="Minutes", yaxis_title="Number of Users",
        height=320,
        paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
        font=dict(color="white"),
    )
    st.plotly_chart(fig_ttc, use_container_width=True)

    st.divider()

    # ── Dropout Profiles ──────────────────────────────────────────────────────
    st.subheader("🔎 Dropout Profile Analysis")
    st.caption("Who are the users dropping off? Surfaces survivorship bias.")

    dropout_df = query(
        "SELECT * FROM funnel_dropout_profiles ORDER BY dropout_stage, users DESC"
    )

    stage_labels = {
        "signup_dropout":  "Dropped before Signup",
        "profile_dropout": "Dropped before Profile Complete",
        "apply_dropout":   "Dropped before Apply",
    }

    selected_stage = st.selectbox(
        "Select dropout stage",
        list(stage_labels.keys()),
        format_func=lambda k: stage_labels[k],
    )

    stage_data = dropout_df[dropout_df["dropout_stage"] == selected_stage]

    col_d1, col_d2 = st.columns(2)

    with col_d1:
        by_device = stage_data.groupby("device")["users"].sum().reset_index()
        fig_dev = go.Figure(go.Bar(
            x=by_device["device"], y=by_device["users"],
            marker_color="#EF553B",
            text=by_device["users"], textposition="outside",
        ))
        fig_dev.update_layout(
            title="Dropouts by Device", height=300,
            paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_dev, use_container_width=True)

    with col_d2:
        by_source = stage_data.groupby("source")["users"].sum().reset_index()
        fig_src = go.Figure(go.Bar(
            x=by_source["source"], y=by_source["users"],
            marker_color="#FFC300",
            text=by_source["users"], textposition="outside",
        ))
        fig_src.update_layout(
            title="Dropouts by Acquisition Source", height=300,
            paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_src, use_container_width=True)

    st.info(
        "**Survivorship bias check**: If mobile users dominate dropout profiles "
        "but desktop users dominate funnel survivors, your mobile UX is the "
        "real conversion problem — not your overall product."
    )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — COHORT RETENTION
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔁 Cohort Retention":
    st.title("🔁 Cohort Retention Analysis")
    st.caption("Weekly cohorts — % of users still active N weeks after signup")

    ret_df = query("SELECT * FROM cohort_retention ORDER BY cohort_week, week_number")

    if ret_df.empty:
        st.warning("No cohort data available. Re-run the pipeline.")
        st.stop()

    max_week  = int(ret_df["week_number"].max())
    total_rows = len(ret_df)
    st.caption(f"Data check: {total_rows} rows, max week_number = {max_week}")

    # ── Heatmap ───────────────────────────────────────────────────────────────
    st.subheader("Cohort Retention Heatmap")

    pivot = ret_df.pivot_table(
        index="cohort_week", columns="week_number",
        values="retention_rate_pct", aggfunc="mean",
    ).round(1)
    pivot = pivot.sort_index()
    valid_weeks = [c for c in pivot.columns if 0 <= c <= 11]
    pivot = pivot[valid_weeks]

    text_values = [
        [f"{v:.1f}%" if v > 0 else "" for v in row]
        for row in pivot.fillna(0).values
    ]

    fig = go.Figure(go.Heatmap(
        z=pivot.fillna(0).values,
        x=[f"Week {int(c)}" for c in valid_weeks],
        y=pivot.index.tolist(),
        colorscale="RdYlGn",
        zmin=0, zmax=100,
        text=text_values,
        texttemplate="%{text}",
        hovertemplate="Cohort: %{y}<br>%{x}<br>Retention: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Retention %"),
    ))
    fig.update_layout(
        xaxis_title="Week Number Since Signup",
        yaxis_title="Signup Cohort (Week)",
        height=560,
        paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
        font=dict(color="white"),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Retention Curves ──────────────────────────────────────────────────────
    st.subheader("Retention Curves — First 4 Cohorts")

    cohorts_with_data = (
        ret_df[ret_df["week_number"] > 0]["cohort_week"]
        .value_counts().index[:4].tolist()
    )

    if not cohorts_with_data:
        st.warning("Not enough multi-week data. Re-run the pipeline.")
    else:
        fig2 = go.Figure()
        colors_line = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA"]

        for i, cw in enumerate(sorted(cohorts_with_data)):
            subset = ret_df[ret_df["cohort_week"] == cw].sort_values("week_number")
            fig2.add_trace(go.Scatter(
                x=subset["week_number"], y=subset["retention_rate_pct"],
                mode="lines+markers", name=f"Cohort {cw}",
                line=dict(color=colors_line[i % 4], width=2),
                marker=dict(size=6),
            ))

        fig2.update_layout(
            xaxis_title="Week Number", yaxis_title="Retention Rate %",
            xaxis=dict(tickmode="linear", tick0=0, dtick=1, range=[-0.2, max_week + 0.5]),
            yaxis=dict(range=[0, 105]),
            height=380,
            paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
            font=dict(color="white"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig2, use_container_width=True)

        w1_data = ret_df[ret_df["week_number"] == 1]["retention_rate_pct"]
        w4_data = ret_df[ret_df["week_number"] == 4]["retention_rate_pct"]

        if not w1_data.empty and not w4_data.empty:
            st.info(
                f"**Week-1 avg retention**: {w1_data.mean():.1f}% → "
                f"**Week-4 avg retention**: {w4_data.mean():.1f}%. "
                "Steep early drop-off is typical — Week 1 interventions yield highest ROI."
            )

    st.divider()

    # ── Behavioral Cohort Retention ───────────────────────────────────────────
    st.subheader("🧠 Behavioral Cohort Retention")
    st.caption(
        "Retention split by what users DID — not just when they joined. "
        "Inspired by Slack's '2,000 messages' activation research."
    )

    bcr_df = query(
        "SELECT * FROM behavioral_cohort_retention ORDER BY behavior_group, week_number"
    )

    if not bcr_df.empty:
        fig_bcr = go.Figure()
        group_colors = {
            "Activated & Responded":     "#00C9A7",
            "Activated but Ghosted":     "#FFC300",
            "Applied but Not Activated": "#FF6B35",
            "Never Applied":             "#EF553B",
        }
        for group in bcr_df["behavior_group"].unique():
            subset = bcr_df[bcr_df["behavior_group"] == group].sort_values("week_number")
            fig_bcr.add_trace(go.Scatter(
                x=subset["week_number"],
                y=subset["retention_rate_pct"],
                mode="lines+markers",
                name=group,
                line=dict(color=group_colors.get(group, "#888"), width=2),
                marker=dict(size=6),
            ))
        fig_bcr.update_layout(
            xaxis_title="Week Number",
            yaxis_title="Retention Rate %",
            xaxis=dict(tickmode="linear", tick0=0, dtick=1),
            yaxis=dict(range=[0, 105]),
            height=400,
            paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
            font=dict(color="white"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig_bcr, use_container_width=True)

        st.success(
            "**Key insight**: 'Activated & Responded' users retain at the highest rate. "
            "This is your platform's activation threshold — the equivalent of Slack's 2,000 messages. "
            "Every product decision should push users toward this behavioral state."
        )

    st.divider()

    # ── Raw Table ─────────────────────────────────────────────────────────────
    st.subheader("Raw Retention Data")
    with st.expander("View full retention table"):
        st.dataframe(
            ret_df.rename(columns={
                "cohort_week": "Cohort Week", "week_number": "Week #",
                "active_users": "Active Users", "cohort_size": "Cohort Size",
                "retention_rate_pct": "Retention %",
            }),
            use_container_width=True, hide_index=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — SEGMENTATION
# ══════════════════════════════════════════════════════════════════════════════
elif page == "👥 Segmentation":
    st.title("👥 User Segmentation")
    st.caption("Low / Medium / High engagement breakdown with KPI comparison")

    seg_df = query("""
        SELECT
            engagement_segment,
            COUNT(*)                              AS users,
            ROUND(AVG(is_activated)*100, 1)       AS activation_rate,
            ROUND(AVG(is_ghosted)*100, 1)         AS ghosting_rate,
            ROUND(AVG(is_churned)*100, 1)         AS churn_rate,
            ROUND(AVG(job_views), 1)              AS avg_job_views,
            ROUND(AVG(job_applies), 1)            AS avg_applies,
            ROUND(AVG(relevance_score), 3)        AS avg_relevance
        FROM user_summary
        GROUP BY engagement_segment
        ORDER BY CASE engagement_segment
            WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END
    """)

    metric_labels = ["Activation Rate %", "Ghosting Rate %", "Churn Rate %"]
    seg_colors = {"high": "#00C9A7", "medium": "#FFC300", "low": "#FF5733"}

    fig = go.Figure()
    for _, row in seg_df.iterrows():
        fig.add_trace(go.Bar(
            name=row["engagement_segment"].title(),
            x=metric_labels,
            y=[row["activation_rate"], row["ghosting_rate"], row["churn_rate"]],
            marker_color=seg_colors.get(row["engagement_segment"], "#888"),
            text=[f"{v}%" for v in [row["activation_rate"], row["ghosting_rate"], row["churn_rate"]]],
            textposition="outside",
        ))
    fig.update_layout(
        barmode="group",
        title="KPI Comparison by Engagement Segment",
        yaxis_title="Rate (%)",
        height=420,
        paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
        font=dict(color="white"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Segment Detail Table")
    st.dataframe(
        seg_df.rename(columns={
            "engagement_segment": "Segment", "users": "Users",
            "activation_rate": "Activation %", "ghosting_rate": "Ghosting %",
            "churn_rate": "Churn %", "avg_job_views": "Avg Views",
            "avg_applies": "Avg Applies", "avg_relevance": "Avg Relevance",
        }),
        use_container_width=True, hide_index=True,
    )

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.warning(
            "**Low-engagement users** are most at risk — high churn, low activation. "
            "Targeted onboarding nudges can move them to medium segment."
        )
    with col2:
        st.info(
            "**High-engagement users** still experience significant ghosting. "
            "Even motivated users lose trust when recruiters don't respond."
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — GHOSTING & CHURN
# ══════════════════════════════════════════════════════════════════════════════
elif page == "👻 Ghosting & Churn":
    st.title("👻 Ghosting & Churn Intelligence")
    st.caption("Understanding how recruiter silence drives platform abandonment")

    matrix_df = query("SELECT * FROM ghosting_churn_matrix")
    kpi = query("SELECT * FROM kpi_summary").iloc[0]

    st.subheader("Churn Rate: Ghosted vs Non-Ghosted Applicants")

    ghosted_churn     = matrix_df[matrix_df["is_ghosted"] == 1]
    not_ghosted_churn = matrix_df[matrix_df["is_ghosted"] == 0]

    def get_pct(df, churned_val):
        row = df[df["is_churned"] == churned_val]
        return float(row["pct_within_ghost_group"].values[0]) if not row.empty else 0

    gc_ghosted     = get_pct(ghosted_churn, 1)
    gc_not_ghosted = get_pct(not_ghosted_churn, 1)

    fig = go.Figure()
    categories = ["Ghosted Applicants", "Non-Ghosted Applicants"]
    fig.add_trace(go.Bar(
        name="Churned", x=categories,
        y=[gc_ghosted, gc_not_ghosted],
        marker_color="#FF5733",
        text=[f"{gc_ghosted:.1f}%", f"{gc_not_ghosted:.1f}%"],
        textposition="inside",
    ))
    fig.add_trace(go.Bar(
        name="Retained", x=categories,
        y=[100 - gc_ghosted, 100 - gc_not_ghosted],
        marker_color="#00C9A7",
        text=[f"{100-gc_ghosted:.1f}%", f"{100-gc_not_ghosted:.1f}%"],
        textposition="inside",
    ))
    fig.update_layout(
        barmode="stack",
        title="Churn Rate Comparison: Ghosted vs Non-Ghosted Users",
        yaxis_title="% of Applicants",
        yaxis=dict(range=[0, 105]),
        height=420,
        paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
        font=dict(color="white"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("User Pathway Sankey: Applied → Ghosted → Churned")

    total_applicants = int(
        query("SELECT COUNT(*) AS n FROM user_summary WHERE did_apply = 1")["n"].iloc[0]
    )
    ghosted_total     = int(kpi["ghosted_users"])
    not_ghosted_total = total_applicants - ghosted_total

    ghosted_churned = int(
        matrix_df[(matrix_df["is_ghosted"] == 1) & (matrix_df["is_churned"] == 1)]["user_count"].sum()
    )
    ghosted_retained = ghosted_total - ghosted_churned
    ng_churned = int(
        matrix_df[(matrix_df["is_ghosted"] == 0) & (matrix_df["is_churned"] == 1)]["user_count"].sum()
    )
    ng_retained = not_ghosted_total - ng_churned

    fig_sankey = go.Figure(go.Sankey(
        node=dict(
            pad=20, thickness=20,
            line=dict(color="white", width=0.5),
            label=["All Applicants", "Ghosted", "Not Ghosted",
                   "Churned (post-ghost)", "Retained (post-ghost)",
                   "Churned (no ghost)", "Retained (no ghost)"],
            color=["#636EFA", "#EF553B", "#00CC96",
                   "#FF5733", "#00C9A7", "#FFC300", "#55A868"],
        ),
        link=dict(
            source=[0, 0, 1, 1, 2, 2],
            target=[1, 2, 3, 4, 5, 6],
            value=[ghosted_total, not_ghosted_total,
                   ghosted_churned, ghosted_retained,
                   ng_churned, ng_retained],
            color=[
                "rgba(239,85,59,0.4)", "rgba(0,204,150,0.4)",
                "rgba(255,87,51,0.5)", "rgba(0,201,167,0.4)",
                "rgba(255,195,0,0.4)", "rgba(85,168,104,0.4)",
            ],
        ),
    ))
    fig_sankey.update_layout(
        title="Ghosting → Churn Pathway", height=480,
        paper_bgcolor="#0E1117", font=dict(color="white", size=13),
    )
    st.plotly_chart(fig_sankey, use_container_width=True)

    st.divider()
    st.subheader("📌 Key Findings")
    col1, col2 = st.columns(2)
    delta = gc_ghosted - gc_not_ghosted
    with col1:
        st.error(
            f"**Ghosting amplifies churn by ~{delta:.0f} percentage points.** "
            f"Ghosted applicants churn at {gc_ghosted:.1f}% vs "
            f"{gc_not_ghosted:.1f}% for responded applicants."
        )
        st.warning(
            "**Trust erosion loop**: User applies → gets ghosted → loses faith → "
            "stops logging in → churns. Breaking this requires SLA enforcement "
            "on recruiter response times."
        )
    with col2:
        st.info(
            "**Recruiter accountability**: Platforms must surface recruiter "
            "response rates to help job-seekers make better application decisions."
        )
        st.success(
            "**Intervention point**: Send application status nudges to recruiters "
            "at 72h and 7d marks. Automated closure emails reduce uncertainty "
            "and significantly reduce ghosting-driven churn."
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — LTV & REVENUE
# ══════════════════════════════════════════════════════════════════════════════
elif page == "💰 LTV & Revenue":
    st.title("💰 LTV & Revenue Intelligence")
    st.caption(
        "Connecting behavioral signals to estimated platform revenue. "
        "Model: interview_invite = $150 placement fee | job_apply = $5 data signal | activated = $20 premium"
    )

    ltv_df   = query("SELECT * FROM ltv_cac_summary ORDER BY ltv_cac_ratio DESC")
    proxy_df = query("SELECT * FROM ltv_proxy")
    kpi      = query("SELECT * FROM kpi_summary").iloc[0]

    # ── Summary KPIs ──────────────────────────────────────────────────────────
    total_ltv   = proxy_df["estimated_ltv_usd"].sum()
    ghosted_ltv = proxy_df[proxy_df["is_ghosted"] == 1]["estimated_ltv_usd"].sum()
    churn_ltv   = proxy_df[proxy_df["is_churned"] == 1]["estimated_ltv_usd"].sum()
    avg_ltv     = proxy_df["estimated_ltv_usd"].mean()

    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Total Platform LTV",     f"${total_ltv:,.0f}")
    r2.metric("Avg LTV per User",       f"${avg_ltv:,.2f}")
    r3.metric("LTV at Risk (Ghosted)",  f"${ghosted_ltv:,.0f}",
              delta=f"-${ghosted_ltv:,.0f}", delta_color="inverse")
    r4.metric("LTV Lost to Churn",      f"${churn_ltv:,.0f}",
              delta=f"-${churn_ltv:,.0f}", delta_color="inverse")

    st.divider()

    # ── LTV:CAC Table ─────────────────────────────────────────────────────────
    st.subheader("LTV:CAC Ratio by Segment & Acquisition Source")
    st.caption("Industry benchmark for healthy SaaS: LTV:CAC ≥ 3.0")

    def color_ltv(val):
        if pd.isna(val):
            return ""
        if val >= 3.0:
            return "background-color: #00C9A7; color: black"
        if val >= 1.5:
            return "background-color: #FFC300; color: black"
        return "background-color: #FF5733; color: white"

    st.dataframe(
        ltv_df.rename(columns={
            "engagement_segment": "Segment", "source": "Source",
            "users": "Users", "avg_ltv": "Avg LTV ($)",
            "avg_cac": "Avg CAC ($)", "ltv_cac_ratio": "LTV:CAC",
            "total_ltv": "Total LTV ($)",
        }).style.map(color_ltv, subset=["LTV:CAC"]),
        use_container_width=True, hide_index=True,
    )

    st.divider()

    # ── LTV Distribution ──────────────────────────────────────────────────────
    st.subheader("LTV Distribution by Engagement Segment")

    fig_ltv = go.Figure()
    seg_colors = {"high": "#00C9A7", "medium": "#FFC300", "low": "#FF5733"}
    for seg in ["high", "medium", "low"]:
        seg_data = proxy_df[proxy_df["engagement_segment"] == seg]["estimated_ltv_usd"]
        fig_ltv.add_trace(go.Box(
            y=seg_data, name=seg.title(),
            marker_color=seg_colors[seg], boxmean=True,
        ))
    fig_ltv.update_layout(
        yaxis_title="Estimated LTV ($)", height=380,
        paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
        font=dict(color="white"),
    )
    st.plotly_chart(fig_ltv, use_container_width=True)

    st.divider()

    # ── Revenue at Risk ───────────────────────────────────────────────────────
    st.subheader("💸 Revenue at Risk Breakdown")

    fig_risk = go.Figure(go.Bar(
        x=["Total LTV", "Ghosted Users LTV", "Churned Users LTV"],
        y=[total_ltv, ghosted_ltv, churn_ltv],
        marker_color=["#636EFA", "#FFC300", "#FF5733"],
        text=[f"${v:,.0f}" for v in [total_ltv, ghosted_ltv, churn_ltv]],
        textposition="outside",
    ))
    fig_risk.update_layout(
        title="Platform LTV vs Revenue at Risk",
        yaxis_title="Estimated USD",
        height=360,
        paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
        font=dict(color="white"),
    )
    st.plotly_chart(fig_risk, use_container_width=True)

    st.error(
        f"**${churn_ltv:,.0f} in LTV is sitting in churned users** who haven't returned. "
        "Re-engagement campaigns targeting ghosted users who haven't logged in for 30+ days "
        "are the single highest-ROI intervention available to the platform."
    )
    st.info(
        "**Model note**: LTV figures are proxies based on estimated marketplace economics. "
        "In a real system these would be replaced by actual recruiter fee data and "
        "subscription revenue per user."
    )