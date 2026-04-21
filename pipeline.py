from __future__ import annotations

import logging
import sys

from src.config import validate_config
from src.db import get_connection
from src.extract import load_raw_data, register_tables
from src.features import build_churn_features
from src.load import export_tables
from src.quality import generate_quality_report
from src.sql_runner import run_sql_validations
from src.transform import (
    build_customer_360,
    clean_campaigns,
    clean_interactions,
    clean_transactions,
)
from src.utils import setup_logger


def main() -> int:
    # Inicializa logging do projeto
    setup_logger()
    logger = logging.getLogger(__name__)

    # Conexão começa como None pra garantir fechamento no finally
    con = None

    try:
        logger.info("🚀 Starting DuckDB pipeline")

        # Valida config (paths, variáveis, etc.)
        validate_config()
        logger.info("Configuration validated successfully")

        # Carrega dados brutos (csv → memória)
        data = load_raw_data()

        # Abre conexão com DuckDB e registra tabelas raw
        con = get_connection()
        register_tables(con, data)
        logger.info("Raw tables registered in DuckDB")

        # Roda diagnóstico inicial de qualidade (antes de qualquer transformação)
        logger.info("Running data quality report")
        generate_quality_report(con)

        # Camada de transformação (clean + modelagem base)
        logger.info("Building cleaned analytical tables")
        build_customer_360(con)
        clean_interactions(con)
        clean_transactions(con)
        clean_campaigns(con)  # tabela auxiliar usada nas análises de campanha

        # Camada de feature engineering (dataset final analítico)
        logger.info("Building churn feature table")
        build_churn_features(con)

        # Validação final com queries controladas (Q1–Q4)
        logger.info("Running SQL validations")
        run_sql_validations(con)

        # Exporta datasets finais (provavelmente pra csv/parquet)
        logger.info("Exporting output datasets")
        export_tables(con)

        logger.info("📁 Outputs exported successfully")
        logger.info("✅ Pipeline finished successfully")
        return 0

    except Exception:
        # Loga erro completo (stacktrace) pra facilitar debug
        logger.exception("❌ Pipeline execution failed")
        return 1

    finally:
        # Garante fechamento da conexão independente de erro/sucesso
        if con is not None:
            con.close()
            logger.info("DuckDB connection closed")


if __name__ == "__main__":
    sys.exit(main())