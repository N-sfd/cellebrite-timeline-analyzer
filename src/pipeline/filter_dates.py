import pandas as pd

def filter_to_window(df: pd.DataFrame, start: str, end: str, tz: str) -> pd.DataFrame:
    """
    start/end are interpreted in timezone tz, then converted to UTC for filtering.
    """
    df = df.copy()
    df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], utc=True)

    start_local = pd.Timestamp(start).tz_localize(tz)
    end_local = pd.Timestamp(end).tz_localize(tz)

    start_utc = start_local.tz_convert("UTC")
    end_utc = end_local.tz_convert("UTC")

    out = df[(df["event_time_utc"] >= start_utc) & (df["event_time_utc"] < end_utc)]
    return out.sort_values("event_time_utc")