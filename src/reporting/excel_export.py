import pandas as pd

def export_excel(df, output_path):
    writer = pd.ExcelWriter(output_path, engine="openpyxl")

    df.to_excel(writer, sheet_name="Full Timeline", index=False)

    # summary sheet
    summary = df.groupby("action").size().reset_index(name="count")
    summary.to_excel(writer, sheet_name="Event Summary", index=False)

    actor_summary = df.groupby("actor_hint").size().reset_index(name="count")
    actor_summary.to_excel(writer, sheet_name="Actor Summary", index=False)

    writer.close()