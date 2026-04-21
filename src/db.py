import duckdb
from pathlib import Path
from .config import Config

def get_connection():
    Path(str(Config.DB_PATH)).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(Config.DB_PATH))