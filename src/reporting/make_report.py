from pathlib import Path
import pandas as pd

def write_report(df: pd.DataFrame, out_dir: Path, start: str, end: str, tz: str) -> Path:
    # basic summaries
    top_sources = df["source"].value_counts().head(10)
    top_actions = df["action"].value_counts()
    top_actor = df["actor_hint"].value_counts()

    md = []
    md.append(f"# Cellebrite Export Timeline Report\n")
    md.append(f"**Window:** {start} → {end} ({tz})\n")
    md.append(f"**Total events in window:** {len(df)}\n")

    md.append("## Event breakdown\n")
    md.append("**By action**\n")
    for k, v in top_actions.items():
        md.append(f"- {k}: {v}\n")

    md.append("\n**Likely actor**\n")
    for k, v in top_actor.items():
        md.append(f"- {k}: {v}\n")

    md.append("\n## Top sources (where events came from)\n")
    for k, v in top_sources.items():
        md.append(f"- {k}: {v}\n")

    # show a sample timeline excerpt
    md.append("\n## Timeline (first 50 events)\n")
    if df.empty:
        md.append("*No events in window.*\n")
    else:
        cols = ["event_time_utc", "action", "actor_hint", "artifact_type", "details", "source"]
        sample = df.head(50)[[c for c in cols if c in df.columns]]
        md.append(sample.to_markdown(index=False))

    report_path = out_dir / "report.md"
    report_path.write_text("\n".join(md), encoding="utf-8")
    return report_path