from pathlib import Path
import pandas as pd

def inventory_files(root: Path) -> pd.DataFrame:
    rows = []
    for p in root.rglob("*"):
        if p.is_file():
            rows.append({
                "path": str(p.relative_to(root)),
                "ext": p.suffix.lower(),
                "size_bytes": p.stat().st_size,
            })
    df = pd.DataFrame(rows, columns=["path", "ext", "size_bytes"])
    if df.empty:
        return df
    return df.sort_values(["ext", "size_bytes"], ascending=[True, False])