from pathlib import Path
import typer
from rich import print

from src.pipeline.inventory import inventory_files
from src.pipeline.build_timeline import build_timeline
from src.pipeline.filter_dates import filter_to_window
from src.reporting.make_report import write_report

app = typer.Typer(add_completion=False)


@app.callback()
def main_callback():
    """Cellebrite timeline analyzer."""


def _run_pipeline(
    input_dir: str,
    start: str,
    end: str,
    tz: str,
    out_dir: str,
) -> None:
    input_path = Path(input_dir)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    print("[bold cyan]1) Inventory files...[/bold cyan]")
    inv = inventory_files(input_path)
    inv.to_csv(out_path / "inventory.csv", index=False)
    print(f"Saved: {out_path/'inventory.csv'}")

    print("[bold cyan]2) Build unified timeline...[/bold cyan]")
    timeline = build_timeline(input_path)
    timeline.to_csv(out_path / "timeline_all.csv", index=False)
    print(f"Saved: {out_path/'timeline_all.csv'} ({len(timeline)} rows)")

    print("[bold cyan]3) Filter to window...[/bold cyan]")
    filtered = filter_to_window(timeline, start=start, end=end, tz=tz)
    filtered.to_csv(out_path / "timeline_window.csv", index=False)
    print(f"Saved: {out_path/'timeline_window.csv'} ({len(filtered)} rows)")

    print("[bold cyan]4) Write report...[/bold cyan]")
    report_path = write_report(filtered, out_path, start, end, tz)
    print(f"Saved: {report_path}")


@app.command("run")
def run(
    input_dir: str = typer.Option("data", help="Folder containing Cellebrite export"),
    start: str = typer.Option(..., help="Start datetime (e.g., 2026-03-01 00:00)"),
    end: str = typer.Option(..., help="End datetime (exclusive) (e.g., 2026-03-03 00:00)"),
    tz: str = typer.Option("UTC", help="Timezone of provided start/end (e.g., America/New_York)"),
    out_dir: str = typer.Option("outputs", help="Output folder"),
):
    _run_pipeline(input_dir, start, end, tz, out_dir)

if __name__ == "__main__":
    app()