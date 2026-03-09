import pandas as pd

POWER_KEYWORDS = ["boot", "power", "startup", "device_boot", "powered on"]

def find_power_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    text_cols = [c for c in ["action", "details", "artifact_type", "source"] if c in df.columns]

    if not text_cols:
        return df.iloc[0:0].copy()

    mask = pd.Series(False, index=df.index)

    for col in text_cols:
        col_mask = df[col].astype(str).str.lower().apply(
            lambda x: any(keyword in x for keyword in POWER_KEYWORDS)
        )
        mask = mask | col_mask

    return df[mask].copy()


def get_activity_around_power_events(
    df: pd.DataFrame,
    minutes_before: int = 5,
    minutes_after: int = 10
) -> pd.DataFrame:
    if df.empty or "event_time_utc" not in df.columns:
        return pd.DataFrame()

    working = df.copy()
    working["event_time_utc"] = pd.to_datetime(working["event_time_utc"], errors="coerce", utc=True)
    working = working.dropna(subset=["event_time_utc"]).sort_values("event_time_utc")

    power_events = find_power_events(working)
    if power_events.empty:
        return pd.DataFrame()

    correlated_frames = []

    for idx, power_row in power_events.iterrows():
        boot_time = power_row["event_time_utc"]
        start_time = boot_time - pd.Timedelta(minutes=minutes_before)
        end_time = boot_time + pd.Timedelta(minutes=minutes_after)

        window_df = working[
            (working["event_time_utc"] >= start_time) &
            (working["event_time_utc"] <= end_time)
        ].copy()

        window_df["power_event_time"] = boot_time
        window_df["minutes_from_power_event"] = (
            (window_df["event_time_utc"] - boot_time).dt.total_seconds() / 60.0
        ).round(2)

        window_df["is_power_event"] = window_df.index == idx
        correlated_frames.append(window_df)

    if not correlated_frames:
        return pd.DataFrame()

    result = pd.concat(correlated_frames, ignore_index=True)
    return result.sort_values(["power_event_time", "event_time_utc"])