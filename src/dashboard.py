from pathlib import Path
import subprocess
import sys

# Ensure project root is on path when run as script (e.g. streamlit run src/dashboard.py)
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pandas as pd
import streamlit as st
import plotly.express as px

from src.analysis.power_event_correlation import get_activity_around_power_events
from src.analysis.suspicious_burst_detector import detect_suspicious_bursts, get_events_for_burst

st.set_page_config(page_title="Cellebrite Timeline Analyzer", layout="wide")

st.title("Cellebrite Timeline Analyzer")
st.caption("Interactive forensic investigation dashboard")

OUTPUTS = _root / "outputs"
TIMELINE_PATH = OUTPUTS / "timeline_all.csv"

if not TIMELINE_PATH.exists():
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    input_dir = "data/client_export_folder"
    if not (_root / input_dir).exists():
        input_dir = "data/mock_export"
    with st.spinner("Running pipeline to build timeline..."):
        subprocess.run(
            [
                sys.executable,
                "-m",
                "src.main",
                "run",
                "--input-dir",
                input_dir,
                "--start",
                "2026-03-01 00:00",
                "--end",
                "2026-03-03 00:00",
                "--tz",
                "UTC",
                "--out-dir",
                "outputs",
            ],
            check=True,
            cwd=str(_root),
        )
    st.success("Timeline built. Loading dashboard.")

df = pd.read_csv(TIMELINE_PATH)

# If pipeline produced an empty timeline but default data exists, build in-process
if df.empty and (DEFAULT_DATA := _root / "data" / "mock_export").exists():
    with st.spinner("Building timeline from default dataset..."):
        from src.pipeline.build_timeline import build_timeline
        OUTPUTS.mkdir(parents=True, exist_ok=True)
        timeline = build_timeline(DEFAULT_DATA)
        timeline.to_csv(TIMELINE_PATH, index=False)
        df = pd.read_csv(TIMELINE_PATH)
    st.success("Timeline built from data/mock_export.")

if "event_time_utc" in df.columns:
    df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], errors="coerce")

st.sidebar.header("Filters")

source_options = sorted(df["source"].dropna().unique().tolist()) if "source" in df.columns else []
artifact_options = sorted(df["artifact_type"].dropna().unique().tolist()) if "artifact_type" in df.columns else []
action_options = sorted(df["action"].dropna().unique().tolist()) if "action" in df.columns else []
actor_options = sorted(df["actor_hint"].dropna().unique().tolist()) if "actor_hint" in df.columns else []

selected_sources = st.sidebar.multiselect("Source", source_options)
selected_artifacts = st.sidebar.multiselect("Artifact Type", artifact_options)
selected_actions = st.sidebar.multiselect("Action", action_options)
selected_actors = st.sidebar.multiselect("Actor Hint", actor_options)

filtered = df.copy()

if selected_sources:
    filtered = filtered[filtered["source"].isin(selected_sources)]
if selected_artifacts:
    filtered = filtered[filtered["artifact_type"].isin(selected_artifacts)]
if selected_actions:
    filtered = filtered[filtered["action"].isin(selected_actions)]
if selected_actors:
    filtered = filtered[filtered["actor_hint"].isin(selected_actors)]

if "event_time_utc" in filtered.columns and filtered["event_time_utc"].notna().any():
    min_dt = filtered["event_time_utc"].min()
    max_dt = filtered["event_time_utc"].max()

    start_dt, end_dt = st.sidebar.slider(
        "Date range",
        min_value=min_dt.to_pydatetime(),
        max_value=max_dt.to_pydatetime(),
        value=(min_dt.to_pydatetime(), max_dt.to_pydatetime()),
        format="YYYY-MM-DD HH:mm:ss",
    )
    filtered = filtered[
        (filtered["event_time_utc"] >= pd.Timestamp(start_dt)) &
        (filtered["event_time_utc"] <= pd.Timestamp(end_dt))
    ]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Events", len(filtered))
col2.metric("Sources", filtered["source"].nunique() if "source" in filtered.columns else 0)
col3.metric("Actions", filtered["action"].nunique() if "action" in filtered.columns else 0)
col4.metric("Artifacts", filtered["artifact_type"].nunique() if "artifact_type" in filtered.columns else 0)

st.subheader("Event Distribution")

left, right = st.columns(2)

if "action" in filtered.columns and not filtered.empty:
    action_counts = filtered["action"].value_counts().reset_index()
    action_counts.columns = ["action", "count"]
    fig_actions = px.bar(action_counts, x="action", y="count", title="Events by Action")
    left.plotly_chart(fig_actions, use_container_width=True)

if "actor_hint" in filtered.columns and not filtered.empty:
    actor_counts = filtered["actor_hint"].value_counts().reset_index()
    actor_counts.columns = ["actor_hint", "count"]
    fig_actor = px.pie(actor_counts, names="actor_hint", values="count", title="Human vs System/App")
    right.plotly_chart(fig_actor, use_container_width=True)

st.subheader("Timeline View")

if "event_time_utc" in filtered.columns and not filtered.empty:
    timeline_chart = filtered.copy()
    timeline_chart["y"] = timeline_chart["action"].fillna("event") if "action" in timeline_chart.columns else "event"

    fig_timeline = px.scatter(
        timeline_chart,
        x="event_time_utc",
        y="y",
        color="actor_hint" if "actor_hint" in timeline_chart.columns else None,
        hover_data=[c for c in ["source", "artifact_type", "details"] if c in timeline_chart.columns],
        title="Interactive Event Timeline"
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

st.subheader("Power-Up Event Correlation")

power_window = get_activity_around_power_events(
    filtered,
    minutes_before=5,
    minutes_after=10
)

if power_window.empty:
    st.info("No power/boot events found in the current filtered dataset.")
else:
    power_events_list = sorted(
        power_window["power_event_time"].dropna().astype(str).unique().tolist()
    )

    selected_power_event = st.selectbox(
        "Select a power event",
        power_events_list
    )

    selected_window = power_window[
        power_window["power_event_time"].astype(str) == selected_power_event
    ].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric("Events in Window", len(selected_window))
    col2.metric(
        "Likely User Events",
        (selected_window["actor_hint"] == "likely_user").sum() if "actor_hint" in selected_window.columns else 0
    )
    col3.metric(
        "Likely System/App Events",
        (selected_window["actor_hint"] == "likely_system_or_app").sum() if "actor_hint" in selected_window.columns else 0
    )

    if "event_time_utc" in selected_window.columns:
        chart_df = selected_window.copy()
        chart_df["event_label"] = chart_df["action"].fillna("event") if "action" in chart_df.columns else "event"

        fig_power = px.scatter(
            chart_df,
            x="event_time_utc",
            y="event_label",
            color="actor_hint" if "actor_hint" in chart_df.columns else None,
            symbol="is_power_event",
            hover_data=[c for c in ["source", "artifact_type", "details", "minutes_from_power_event"] if c in chart_df.columns],
            title="Activity 5 Minutes Before and 10 Minutes After Power Event"
        )
        st.plotly_chart(fig_power, use_container_width=True)

    st.markdown("### Correlated Event Table")
    st.dataframe(
        selected_window[
            [c for c in [
                "event_time_utc",
                "minutes_from_power_event",
                "action",
                "actor_hint",
                "artifact_type",
                "details",
                "source",
                "is_power_event"
            ] if c in selected_window.columns]
        ],
        use_container_width=True,
        height=350
    )

st.subheader("Suspicious Burst Detector")

burst_df = detect_suspicious_bursts(
    filtered,
    window_minutes=5,
    min_events=4
)

if burst_df.empty:
    st.info("No suspicious bursts detected in the current filtered dataset.")
else:
    st.markdown("### Detected Burst Windows")
    burst_cols = [
        "burst_start",
        "burst_end",
        "event_count",
        "artifact_types",
        "likely_user_events",
        "likely_system_events",
        "burst_score",
        "risk_level"
    ]
    st.dataframe(
        burst_df[[c for c in burst_cols if c in burst_df.columns]],
        use_container_width=True,
        height=220
    )

    risk_counts = burst_df["risk_level"].value_counts().reset_index()
    risk_counts.columns = ["risk", "count"]
    fig_risk = px.bar(
        risk_counts,
        x="risk",
        y="count",
        color="risk",
        title="Suspicious Burst Risk Levels",
        color_discrete_map={
            "HIGH": "red",
            "MEDIUM": "orange",
            "LOW": "yellow",
            "INFO": "green"
        }
    )
    st.plotly_chart(fig_risk, use_container_width=True)

    burst_options = [
        f"{idx} | {row['burst_start']} -> {row['burst_end']} | events={row['event_count']} | score={row['burst_score']}"
        for idx, row in burst_df.iterrows()
    ]

    selected_burst_label = st.selectbox("Select a suspicious burst", burst_options)
    selected_index = int(selected_burst_label.split("|")[0].strip())
    selected_burst = burst_df.iloc[selected_index]

    burst_events = get_events_for_burst(filtered, selected_burst)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events in Burst", int(selected_burst["event_count"]))
    c2.metric("Artifact Types", int(selected_burst["artifact_types"]))
    c3.metric("Likely User", int(selected_burst["likely_user_events"]))
    c4.metric("Likely System/App", int(selected_burst["likely_system_events"]))

    if not burst_events.empty:
        chart_df = burst_events.copy()
        chart_df["event_label"] = (
            chart_df["action"].fillna("event")
            if "action" in chart_df.columns else "event"
        )

        fig_burst = px.scatter(
            chart_df,
            x="event_time_utc",
            y="event_label",
            color="actor_hint" if "actor_hint" in chart_df.columns else None,
            size_max=10,
            hover_data=[c for c in [
                "source", "artifact_type", "details", "minutes_from_burst_start"
            ] if c in chart_df.columns],
            title="Suspicious Burst Activity Timeline"
        )
        st.plotly_chart(fig_burst, use_container_width=True)

        st.markdown("### Events Inside Selected Burst")
        st.dataframe(
            burst_events[
                [c for c in [
                    "event_time_utc",
                    "minutes_from_burst_start",
                    "action",
                    "actor_hint",
                    "artifact_type",
                    "details",
                    "source"
                ] if c in burst_events.columns]
            ],
            use_container_width=True,
            height=320
        )

st.subheader("Investigation Summary")

if burst_df.empty:
    high, medium = 0, 0
else:
    high = (burst_df["risk_level"] == "HIGH").sum()
    medium = (burst_df["risk_level"] == "MEDIUM").sum()

st.write(f"High risk bursts detected: **{high}**")
st.write(f"Medium risk bursts detected: **{medium}**")

if high > 0:
    st.error("⚠ High-risk activity bursts detected. Further investigation recommended.")
elif medium > 0:
    st.warning("Moderate suspicious activity detected.")
else:
    st.success("No significant suspicious bursts detected.")

st.subheader("Filtered Events Table")
st.dataframe(filtered, use_container_width=True, height=400)

csv_data = filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download Filtered CSV",
    data=csv_data,
    file_name="filtered_timeline.csv",
    mime="text/csv",
)