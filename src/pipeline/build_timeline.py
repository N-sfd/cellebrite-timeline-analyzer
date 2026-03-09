from pathlib import Path
import pandas as pd
import sqlite3
from src.utils.timeparse import parse_any_datetime
TS_PRIORITY = [
    "event_time",
    "eventtime",
    "timestamp",
    "time",
    "date",
    "start time",
    "end time",
    "created time",
    "modified time",
    "datetime"
]

def _pick_timestamp_column(cols):
    cols = [c.lower().strip() for c in cols]
    for wanted in TS_PRIORITY:
        if wanted in cols:
            return wanted
    # fallback: any column containing time/date
    for c in cols:
        if "time" in c or "date" in c:
            return c
    return None

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def _guess_action_from_row(row: dict) -> str:
    # Heuristic: look for keywords in known fields
    text = " ".join([str(v) for v in row.values() if v is not None]).lower()
    for k in ["deleted", "remove", "uninstall"]:
        if k in text: return "deleted"
    for k in ["created", "install", "added", "download"]:
        if k in text: return "created"
    for k in ["modified", "updated", "edit", "write"]:
        if k in text: return "modified"
    for k in ["access", "open", "view", "read"]:
        if k in text: return "accessed"
    return "event"

def _actor_hint(row: dict) -> str:
    text = " ".join([str(v) for v in row.values() if v is not None]).lower()
    # user indicators
    if any(k in text for k in ["user", "tapped", "typed", "sent", "outgoing", "foreground", "unlock"]):
        return "likely_user"
    # system/app indicators
    if any(k in text for k in ["system", "service", "background", "sync", "scheduler", "update", "play store"]):
        return "likely_system_or_app"
    return "unknown"

def _events_from_csv(root: Path) -> list[pd.DataFrame]:
    frames = []
    for f in root.rglob("*.csv"):
        try:
            df = pd.read_csv(f, low_memory=False)
        except Exception:
            continue

        df = _normalize_columns(df)
        ts_col = _pick_timestamp_column(df.columns)
        if not ts_col:
            continue

        df["event_time_utc"] = df[ts_col].apply(parse_any_datetime)
        df = df.dropna(subset=["event_time_utc"])

        # Build a compact event record while keeping traceability
        df_events = pd.DataFrame({
            "event_time_utc": df["event_time_utc"],
            "source": str(f.relative_to(root)),
            "artifact_type": f.parent.name,
            "action": df.apply(lambda r: _guess_action_from_row(r.to_dict()), axis=1),
            "actor_hint": df.apply(lambda r: _actor_hint(r.to_dict()), axis=1),
            "details": df.apply(lambda r: _compact_details(r.to_dict()), axis=1),
        })

        frames.append(df_events)

    return frames

def _compact_details(row: dict) -> str:
    # Prefer a few common descriptive fields if present
    preferred = ["name", "filename", "path", "package", "app", "title", "subject", "url", "from", "to", "direction"]
    parts = []
    for k in preferred:
        if k in row and pd.notna(row[k]) and str(row[k]).strip():
            parts.append(f"{k}={row[k]}")
    if parts:
        return " | ".join(parts)[:800]

    # fallback: short stringify
    txt = " ".join([f"{k}={row[k]}" for k in list(row.keys())[:8]])
    return txt[:800]

def _events_from_sqlite(root: Path) -> list[pd.DataFrame]:
    frames = []
    dbs = list(root.rglob("*.db")) + list(root.rglob("*.sqlite")) + list(root.rglob("*.sqlite3"))

    for db in dbs:
        try:
            con = sqlite3.connect(db)
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cur.fetchall()]
        except Exception:
            continue

        for t in tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM '{t}' LIMIT 50000", con)
            except Exception:
                continue
            if df.empty:
                continue

            df = _normalize_columns(df)
            ts_col = _pick_timestamp_column(df.columns)
            if not ts_col:
                continue

            df["event_time_utc"] = df[ts_col].apply(parse_any_datetime)
            df = df.dropna(subset=["event_time_utc"])

            df_events = pd.DataFrame({
                "event_time_utc": df["event_time_utc"],
                "source": f"{db.relative_to(root)}::{t}",
                "artifact_type": "sqlite",
                "action": df.apply(lambda r: _guess_action_from_row(r.to_dict()), axis=1),
                "actor_hint": df.apply(lambda r: _actor_hint(r.to_dict()), axis=1),
                "details": df.apply(lambda r: _compact_details(r.to_dict()), axis=1),
            })
            frames.append(df_events)

        con.close()

    return frames

def build_timeline(root: Path) -> pd.DataFrame:
    frames = []
    frames += _events_from_csv(root)
    frames += _events_from_sqlite(root)

    if not frames:
        return pd.DataFrame(columns=["event_time_utc","source","artifact_type","action","actor_hint","details"])

    timeline = pd.concat(frames, ignore_index=True)
    timeline["event_time_utc"] = pd.to_datetime(timeline["event_time_utc"], utc=True, errors="coerce")
    timeline = timeline.dropna(subset=["event_time_utc"]).sort_values("event_time_utc")
    return timeline