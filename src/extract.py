from pathlib import Path
from typing import Dict

import logging
import pandas as pd

from .config import Config

logger = logging.getLogger(__name__)

# Mantive os nomes dos arquivos centralizados aqui para evitar string solta no código.
RAW_FILES = {
    "customers": "raw_customers.csv",
    "interactions": "raw_interactions.csv",
    "transactions": "raw_transactions.csv",
    "campaigns": "raw_campaigns.csv",
}

BASE_PATH = Path(str(Config.RAW_DATA_PATH))


def load_csv(file_name: str) -> pd.DataFrame:
    """
    Lê um CSV da pasta raw, normaliza os nomes das colunas e faz validações básicas.
    """
    path = BASE_PATH / file_name

    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"Empty file: {path}")

    # Padronizo os nomes das colunas logo na entrada para evitar
    # inconsistência entre arquivos e reduzir chance de erro no restante do pipeline.
    df.columns = df.columns.str.lower().str.strip()

    logger.info("Loaded %s with %s rows and %s columns", file_name, len(df), len(df.columns))
    return df


def load_raw_data() -> Dict[str, pd.DataFrame]:
    """
    Carrega todos os arquivos brutos esperados pelo exercício.
    """
    data = {table_name: load_csv(file_name) for table_name, file_name in RAW_FILES.items()}

    logger.info("All raw datasets loaded successfully")
    return data


def register_tables(con, dfs: Dict[str, pd.DataFrame]) -> None:
    """
    Registra os DataFrames no DuckDB para uso nas transformações SQL.
    """
    for table_name, df in dfs.items():
        # Se a tabela já estiver registrada na sessão, removo antes
        # para evitar conflito em reexecução do pipeline.
        try:
            con.unregister(table_name)
        except Exception:
            pass

        con.register(table_name, df)
        logger.info("Registered table '%s' in DuckDB with %s rows", table_name, len(df))