from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Cellebrite Timeline Analyzer", layout="wide")

st.title("Cellebrite Timeline Analyzer")
st.caption("Interactive forensic investigation dashboard")

OUTPUTS = Path("outputs")
timeline_path = OUTPUTS / "timeline_all.csv"

if not timeline_path.exists():
    st.error("timeline_all.csv not found in outputs/. Run the pipeline first.")
    st.stop()

df = pd.read_csv(timeline_path)

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

st.subheader("Filtered Events Table")
st.dataframe(filtered, use_container_width=True, height=400)

csv_data = filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download Filtered CSV",
    data=csv_data,
    file_name="filtered_timeline.csv",
    mime="text/csv",
)