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
    # Power/boot events (for correlation panel)
    for k in ["device_boot", "boot", "power_on", "powered on", "startup", "power on"]:
        if k in text:
            return "device_boot"
    for k in ["device_shutdown", "shutdown", "powered off", "power off", "power_off"]:
        if k in text:
            return "device_shutdown"
    for k in ["deleted", "remove", "uninstall"]:
        if k in text: return "deleted"
    for k in ["created", "install", "added", "download"]:
        if k in text: return "created"
    for k in ["modified", "updated", "edit", "write"]:
        if k in text: return "modified"
    for k in ["access", "open", "view", "read"]:
        if k in text: return "accessed"
    # Preserve event_type from device_events.csv (e.g. screen_unlock, screen_lock)
    event_type = row.get("event_type") or row.get("event type")
    if event_type and isinstance(event_type, str) and str(event_type).strip():
        return str(event_type).strip().lower()
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
        except Exception as e:
            print(f"Skipping {f}: {e}")
            continue

        if df.empty:
            continue

        df = _normalize_columns(df)
        print(f"Processing {f.name} with columns: {list(df.columns)}")

        ts_col = _pick_timestamp_column(df.columns)
        if not ts_col:
            print(f"No timestamp column found in {f.name}")
            continue

        df["event_time_utc"] = df[ts_col].apply(parse_any_datetime)
        df = df.dropna(subset=["event_time_utc"])

        if df.empty:
            print(f"No valid timestamps in {f.name}")
            continue

        def make_details(row):
            keep_cols = [c for c in df.columns if c not in ["event_time_utc"]]
            parts = []
            for c in keep_cols[:8]:
                val = row.get(c)
                if pd.notna(val):
                    parts.append(f"{c}={val}")
            return " | ".join(parts)[:800]

        def make_action(row):
            for key in ["event_type", "action", "call_type", "direction"]:
                if key in row and pd.notna(row[key]):
                    return str(row[key]).lower()
            return "event"

        def make_actor_hint(row):
            row_text = " ".join([str(v).lower() for v in row.values if pd.notna(v)])
            user_keys = ["outgoing", "message_sent", "photo_taken", "screen_unlock", "document_edit", "app_opened"]
            system_keys = ["device_boot", "powered on", "background", "sync", "update", "play store"]

            if any(k in row_text for k in user_keys):
                return "likely_user"
            if any(k in row_text for k in system_keys):
                return "likely_system_or_app"
            return "unknown"

        df_events = pd.DataFrame({
            "event_time_utc": df["event_time_utc"],
            "source": str(f.relative_to(root)),
            "artifact_type": f.stem,
            "action": df.apply(make_action, axis=1),
            "actor_hint": df.apply(make_actor_hint, axis=1),
            "details": df.apply(make_details, axis=1),
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