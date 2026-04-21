from pathlib import Path
from .config import Config

def export_tables(con):
    output_dir = Path(str(Config.OUTPUT_PATH))
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = [
        "customer_360",
        "clean_interactions",
        "clean_transactions",
        "churn_features",
        "clean_campaigns"
    ]

    for table in tables:
        df = con.execute(f"SELECT * FROM {table}").df()
        df.to_csv(output_dir / f"{table}.csv", index=False)