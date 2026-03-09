import pandas as pd

def parse_any_datetime(value):
    """
    Accepts:
    - ISO strings (2026-03-05 10:15:00)
    - Unix seconds (1700000000)
    - Unix milliseconds (1700000000000)
    Returns pandas Timestamp in UTC.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT

    s = str(value).strip()
    if not s:
        return pd.NaT

    # numeric epoch?
    if s.isdigit():
        n = int(s)
        unit = "ms" if n > 10_000_000_000 else "s"
        return pd.to_datetime(n, unit=unit, utc=True, errors="coerce")

    return pd.to_datetime(s, utc=True, errors="coerce")