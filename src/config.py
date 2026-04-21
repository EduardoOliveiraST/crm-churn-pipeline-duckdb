import os
from pathlib import Path


class Config:
    """
    Config central do pipeline.

    - Usa valores padrão para rodar sem .env
    - Permite override via variável de ambiente (nível produção)
    """

    BASE_DIR = Path(__file__).resolve().parent.parent
    
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # caminhos padrão (funciona direto ao clonar)
    RAW_DATA_PATH = Path(os.getenv("RAW_PATH", BASE_DIR / "data" / "raw"))
    OUTPUT_PATH = Path(os.getenv("OUTPUT_PATH", BASE_DIR / "output"))
    DB_PATH = Path(os.getenv("DB_PATH", BASE_DIR / "data" / "db" / "pipeline.duckdb"))

    # arquivos
    CUSTOMERS_FILE = RAW_DATA_PATH / "raw_customers.csv"
    INTERACTIONS_FILE = RAW_DATA_PATH / "raw_interactions.csv"
    TRANSACTIONS_FILE = RAW_DATA_PATH / "raw_transactions.csv"
    CAMPAIGNS_FILE = RAW_DATA_PATH / "raw_campaigns.csv"


def validate_config():
    """
    Garante que tudo necessário existe antes de rodar o pipeline.
    """

    required_files = [
        Config.CUSTOMERS_FILE,
        Config.INTERACTIONS_FILE,
        Config.TRANSACTIONS_FILE,
        Config.CAMPAIGNS_FILE,
    ]

    for file in required_files:
        if not file.exists():
            raise FileNotFoundError(f"Missing required file: {file}")

    # cria pasta output se não existir
    Config.OUTPUT_PATH.mkdir(parents=True, exist_ok=True)