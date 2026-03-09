import pandas as pd


def classify_risk(row):
    score = row["burst_score"]
    events = row["event_count"]
    user_events = row["likely_user_events"]

    if score >= 25 or events >= 8:
        return "HIGH"

    if score >= 15:
        return "MEDIUM"

    if user_events > 0:
        return "LOW"

    return "INFO"


def detect_suspicious_bursts(
    df: pd.DataFrame,
    window_minutes: int = 5,
    min_events: int = 5
) -> pd.DataFrame:
    """
    Finds dense event windows in the timeline.
    A burst is flagged when at least `min_events` occur within `window_minutes`.
    """

    if df.empty or "event_time_utc" not in df.columns:
        return pd.DataFrame()

    working = df.copy()
    working["event_time_utc"] = pd.to_datetime(
        working["event_time_utc"], errors="coerce", utc=True
    )
    working = working.dropna(subset=["event_time_utc"]).sort_values("event_time_utc")
    working = working.reset_index(drop=True)

    bursts = []
    n = len(working)

    for i in range(n):
        start_time = working.loc[i, "event_time_utc"]
        end_time = start_time + pd.Timedelta(minutes=window_minutes)

        window_df = working[
            (working["event_time_utc"] >= start_time) &
            (working["event_time_utc"] <= end_time)
        ].copy()

        if len(window_df) >= min_events:
            artifact_count = (
                window_df["artifact_type"].nunique()
                if "artifact_type" in window_df.columns else 0
            )
            action_count = (
                window_df["action"].nunique()
                if "action" in window_df.columns else 0
            )

            top_action = None
            if "action" in window_df.columns and not window_df["action"].dropna().empty:
                top_action = window_df["action"].value_counts().idxmax()

            top_source = None
            if "source" in window_df.columns and not window_df["source"].dropna().empty:
                top_source = window_df["source"].value_counts().idxmax()

            likely_user = 0
            likely_system = 0
            if "actor_hint" in window_df.columns:
                likely_user = (window_df["actor_hint"] == "likely_user").sum()
                likely_system = (window_df["actor_hint"] == "likely_system_or_app").sum()

            burst_score = (
                len(window_df) * 2 +
                artifact_count * 2 +
                action_count +
                likely_system
            )

            bursts.append({
                "burst_start": start_time,
                "burst_end": window_df["event_time_utc"].max(),
                "window_minutes": window_minutes,
                "event_count": len(window_df),
                "artifact_types": artifact_count,
                "action_types": action_count,
                "likely_user_events": int(likely_user),
                "likely_system_events": int(likely_system),
                "top_action": top_action,
                "top_source": top_source,
                "burst_score": int(burst_score),
            })

    if not bursts:
        return pd.DataFrame()

    burst_df = pd.DataFrame(bursts)

    # Deduplicate overlapping bursts by keeping strongest one for same burst_start
    burst_df = burst_df.sort_values(
        ["burst_start", "burst_score", "event_count"],
        ascending=[True, False, False]
    ).drop_duplicates(subset=["burst_start"])

    # Optional: remove heavy overlap by skipping near-identical windows
    filtered_bursts = []
    last_end = None

    for _, row in burst_df.sort_values("burst_start").iterrows():
        if last_end is None or row["burst_start"] > last_end:
            filtered_bursts.append(row)
            last_end = row["burst_end"]

    result = pd.DataFrame(filtered_bursts)
    result = result.sort_values(["burst_score", "event_count"], ascending=[False, False]).reset_index(drop=True)
    result["risk_level"] = result.apply(classify_risk, axis=1)
    return result


def get_events_for_burst(df: pd.DataFrame, burst_row: pd.Series) -> pd.DataFrame:
    if df.empty or "event_time_utc" not in df.columns:
        return pd.DataFrame()

    working = df.copy()
    working["event_time_utc"] = pd.to_datetime(
        working["event_time_utc"], errors="coerce", utc=True
    )
    working = working.dropna(subset=["event_time_utc"])

    start_time = pd.to_datetime(burst_row["burst_start"], utc=True)
    end_time = pd.to_datetime(burst_row["burst_end"], utc=True)

    burst_events = working[
        (working["event_time_utc"] >= start_time) &
        (working["event_time_utc"] <= end_time)
    ].copy()

    burst_events["minutes_from_burst_start"] = (
        (burst_events["event_time_utc"] - start_time).dt.total_seconds() / 60.0
    ).round(2)

    return burst_events.sort_values("event_time_utc")