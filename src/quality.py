from __future__ import annotations

import logging
from typing import Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

# Data de referência usada nas validações temporais do relatório
REFERENCE_DATE = "2024-06-01"


def _print_title(title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))


def _safe_count(con, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def _safe_df(con, query: str) -> pd.DataFrame:
    return con.execute(query).fetchdf()


def _get_row_counts(con, tables: List[str]) -> Dict[str, int]:
    return {
        table: _safe_count(con, f"SELECT COUNT(*) FROM {table}")
        for table in tables
    }


def _print_row_counts(con) -> Dict[str, int]:
    _print_title("ROW COUNTS")

    # Contagem básica das tabelas brutas para ter visão rápida do volume
    tables = ["customers", "interactions", "transactions", "campaigns"]
    row_counts = _get_row_counts(con, tables)

    for table, count in row_counts.items():
        print(f"{table:<15} {count}")

    return row_counts


def _print_key_nulls(con) -> Dict[str, Dict[str, int]]:
    _print_title("NULL COUNTS (KEY COLUMNS)")

    # Colunas principais que valem a pena monitorar por tabela
    checks = {
        "customers": [
            "customer_id",
            "email",
            "phone",
            "birth_date",
            "created_at",
            "salesforce_id",
        ],
        "interactions": [
            "interaction_id",
            "customer_id",
            "interaction_date",
            "duration_seconds",
            "outcome",
            "campaign_id",
            "source_system",
        ],
        "transactions": [
            "transaction_id",
            "customer_id",
            "transaction_type",
            "amount",
            "transaction_date",
        ],
        "campaigns": [
            "campaign_id",
            "channel",
            "start_date",
            "end_date",
        ],
    }

    results: Dict[str, Dict[str, int]] = {}

    for table, cols in checks.items():
        print(f"\n[{table}]")
        results[table] = {}

        for col in cols:
            cnt = _safe_count(
                con,
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            )
            results[table][col] = cnt
            print(f"{col:<20} {cnt}")

    return results


def _print_referential_integrity(con) -> Dict[str, int]:
    _print_title("REFERENTIAL INTEGRITY")

    # Interações sem correspondência na base de clientes
    orphan_interactions = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM interactions i
        LEFT JOIN customers c
          ON i.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """
    )

    # Transações sem correspondência na base de clientes
    orphan_transactions = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM transactions t
        LEFT JOIN customers c
          ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """
    )

    print(f"interactions without matching customer   {orphan_interactions}")
    print(f"transactions without matching customer  {orphan_transactions}")

    # Amostra rápida para inspecionar quais customer_id estão órfãos
    sample_orphans = _safe_df(
        con,
        """
        SELECT DISTINCT i.customer_id
        FROM interactions i
        LEFT JOIN customers c
          ON i.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        ORDER BY i.customer_id
        LIMIT 10
        """
    )

    if not sample_orphans.empty:
        print("\nSample orphan customer_ids from interactions:")
        print(sample_orphans.to_string(index=False))

    return {
        "orphan_interactions": orphan_interactions,
        "orphan_transactions": orphan_transactions,
    }


def _print_duplicate_email_analysis(con) -> Dict[str, int]:
    _print_title("DUPLICATE EMAIL ANALYSIS")

    # Quantos grupos de email duplicado existem após limpeza básica
    duplicate_groups = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM (
            SELECT LOWER(TRIM(email)) AS cleaned_email
            FROM customers
            WHERE email IS NOT NULL
              AND TRIM(email) <> ''
            GROUP BY LOWER(TRIM(email))
            HAVING COUNT(*) > 1
        ) x
        """
    )

    # Quantas linhas no total são impactadas por esses emails duplicados
    duplicate_rows = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM customers c
        JOIN (
            SELECT LOWER(TRIM(email)) AS cleaned_email
            FROM customers
            WHERE email IS NOT NULL
              AND TRIM(email) <> ''
            GROUP BY LOWER(TRIM(email))
            HAVING COUNT(*) > 1
        ) d
          ON LOWER(TRIM(c.email)) = d.cleaned_email
        """
    )

    print(f"duplicate email groups     {duplicate_groups}")
    print(f"rows affected by duplicate emails  {duplicate_rows}")

    # Top grupos duplicados para facilitar inspeção manual
    sample_duplicates = _safe_df(
        con,
        """
        SELECT
            LOWER(TRIM(email)) AS cleaned_email,
            COUNT(*) AS cnt
        FROM customers
        WHERE email IS NOT NULL
          AND TRIM(email) <> ''
        GROUP BY LOWER(TRIM(email))
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, cleaned_email
        LIMIT 10
        """
    )

    if not sample_duplicates.empty:
        print("\nTop duplicate email groups:")
        print(sample_duplicates.to_string(index=False))

    return {
        "duplicate_email_groups": duplicate_groups,
        "duplicate_email_rows": duplicate_rows,
    }


def _print_customer_anomalies(con) -> Dict[str, int]:
    _print_title("CUSTOMER ANOMALIES")

    # Datas de nascimento inválidas
    invalid_birth_dates = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM customers
        WHERE birth_date IS NOT NULL
          AND TRY_CAST(birth_date AS DATE) IS NULL
        """
    )

    # created_at inválido
    invalid_created_at = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM customers
        WHERE created_at IS NOT NULL
          AND TRY_CAST(created_at AS TIMESTAMP) IS NULL
        """
    )

    # Telefones que continuam inválidos mesmo após remover caracteres não numéricos
    invalid_phone_after_cleaning = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM customers
        WHERE phone IS NOT NULL
          AND TRIM(phone) <> ''
          AND LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) NOT IN (10, 11)
        """
    )

    # Emails ausentes ou em branco
    empty_email = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM customers
        WHERE email IS NULL
           OR TRIM(email) = ''
        """
    )

    print(f"invalid birth_date values          {invalid_birth_dates}")
    print(f"invalid created_at values          {invalid_created_at}")
    print(f"unresolvable phone candidates      {invalid_phone_after_cleaning}")
    print(f"missing/blank email values         {empty_email}")

    return {
        "invalid_birth_dates": invalid_birth_dates,
        "invalid_created_at": invalid_created_at,
        "invalid_phone_candidates": invalid_phone_after_cleaning,
        "blank_email_rows": empty_email,
    }


def _print_interaction_anomalies(con, total_interactions: int) -> Dict[str, float]:
    _print_title("INTERACTION ANOMALIES")

    # Duração negativa normalmente é erro de captura
    negative_durations = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM interactions
        WHERE duration_seconds < 0
        """
    )

    # Datas inválidas em interaction_date
    invalid_interaction_dates = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM interactions
        WHERE interaction_date IS NOT NULL
          AND TRY_CAST(interaction_date AS TIMESTAMP) IS NULL
        """
    )

    # Registros com data futura em relação à data de referência
    future_dated_interactions = _safe_count(
        con,
        f"""
        SELECT COUNT(*)
        FROM interactions
        WHERE TRY_CAST(interaction_date AS TIMESTAMP) > TIMESTAMP '{REFERENCE_DATE} 23:59:59'
        """
    )

    # Fonte legada específica monitorada separadamente
    legacy_v2_count = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM interactions
        WHERE COALESCE(TRIM(source_system), '') = 'LEGACY_V2'
        """
    )

    print(f"negative duration_seconds          {negative_durations}")
    print(f"invalid interaction_date values    {invalid_interaction_dates}")
    print(f"future-dated interactions          {future_dated_interactions}")
    print(f"LEGACY_V2 interaction records      {legacy_v2_count}")

    future_pct = round((future_dated_interactions / total_interactions) * 100, 2) if total_interactions else 0.0
    print(f"future-dated interaction pct       {future_pct}%")

    # Distribuição por source_system para entender origem dos dados
    source_distribution = _safe_df(
        con,
        """
        SELECT
            source_system,
            COUNT(*) AS cnt
        FROM interactions
        GROUP BY source_system
        ORDER BY cnt DESC, source_system
        """
    )

    if not source_distribution.empty:
        print("\nSource system distribution:")
        print(source_distribution.to_string(index=False))

    # Quebra das datas futuras por origem
    future_by_source = _safe_df(
        con,
        f"""
        SELECT
            source_system,
            COUNT(*) AS cnt
        FROM interactions
        WHERE TRY_CAST(interaction_date AS TIMESTAMP) > TIMESTAMP '{REFERENCE_DATE} 23:59:59'
        GROUP BY source_system
        ORDER BY cnt DESC, source_system
        """
    )

    if not future_by_source.empty:
        print("\nFuture-dated interactions by source_system:")
        print(future_by_source.to_string(index=False))

    return {
        "negative_durations": negative_durations,
        "invalid_interaction_dates": invalid_interaction_dates,
        "future_dated_interactions": future_dated_interactions,
        "future_dated_interaction_pct": future_pct,
        "legacy_v2_count": legacy_v2_count,
    }


def _print_transaction_anomalies(con) -> Dict[str, int]:
    _print_title("TRANSACTION ANOMALIES")

    # Datas inválidas em transactions
    invalid_transaction_dates = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM transactions
        WHERE transaction_date IS NOT NULL
          AND TRY_CAST(transaction_date AS TIMESTAMP) IS NULL
        """
    )

    # Compras com amount zerado
    zero_amount_purchases = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM transactions
        WHERE transaction_type = 'purchase'
          AND amount = 0
        """
    )

    # Compras com amount nulo
    null_amount_purchases = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM transactions
        WHERE transaction_type = 'purchase'
          AND amount IS NULL
        """
    )

    # Compras com valor negativo
    negative_purchase_amounts = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM transactions
        WHERE transaction_type = 'purchase'
          AND amount < 0
        """
    )

    print(f"invalid transaction_date values    {invalid_transaction_dates}")
    print(f"purchase amount = 0                {zero_amount_purchases}")
    print(f"purchase amount IS NULL            {null_amount_purchases}")
    print(f"negative purchase amounts          {negative_purchase_amounts}")

    return {
        "invalid_transaction_dates": invalid_transaction_dates,
        "zero_amount_purchases": zero_amount_purchases,
        "null_amount_purchases": null_amount_purchases,
        "negative_purchase_amounts": negative_purchase_amounts,
    }


def _print_campaign_anomalies(con) -> Dict[str, int]:
    _print_title("CAMPAIGN ANOMALIES")

    # start_date inválido
    invalid_start_dates = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM campaigns
        WHERE start_date IS NOT NULL
          AND TRY_CAST(start_date AS TIMESTAMP) IS NULL
        """
    )

    # end_date inválido
    invalid_end_dates = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM campaigns
        WHERE end_date IS NOT NULL
          AND TRY_CAST(end_date AS TIMESTAMP) IS NULL
        """
    )

    # Campanhas com janela invertida
    invalid_windows = _safe_count(
        con,
        """
        SELECT COUNT(*)
        FROM campaigns
        WHERE TRY_CAST(start_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(end_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(start_date AS TIMESTAMP) > TRY_CAST(end_date AS TIMESTAMP)
        """
    )

    print(f"invalid start_date values          {invalid_start_dates}")
    print(f"invalid end_date values            {invalid_end_dates}")
    print(f"start_date > end_date              {invalid_windows}")

    return {
        "invalid_campaign_start_dates": invalid_start_dates,
        "invalid_campaign_end_dates": invalid_end_dates,
        "invalid_campaign_windows": invalid_windows,
    }


def _print_executive_summary(
    row_counts: Dict[str, int],
    ref_integrity: Dict[str, int],
    duplicates: Dict[str, int],
    customer_anomalies: Dict[str, int],
    interaction_anomalies: Dict[str, float],
    transaction_anomalies: Dict[str, int],
    campaign_anomalies: Dict[str, int],
) -> None:
    _print_title("EXECUTIVE SUMMARY")

    print("Main issues detected:")
    print(
        f"- {duplicates['duplicate_email_groups']} duplicate email groups affecting "
        f"{duplicates['duplicate_email_rows']} customer rows."
    )
    print(
        f"- {ref_integrity['orphan_interactions']} interactions and "
        f"{ref_integrity['orphan_transactions']} transactions do not match any customer in the master."
    )
    print(
        f"- {interaction_anomalies['negative_durations']} interactions have negative duration_seconds "
        f"and should be nulled during cleaning."
    )
    print(
        f"- {transaction_anomalies['zero_amount_purchases']} purchase transactions have amount = 0 "
        f"and should be flagged, not dropped."
    )
    print(
        f"- {interaction_anomalies['future_dated_interactions']} interactions "
        f"({interaction_anomalies['future_dated_interaction_pct']}%) are future-dated relative to {REFERENCE_DATE}."
    )
    print(
        f"- {campaign_anomalies['invalid_campaign_windows']} campaigns have invalid windows "
        f"(start_date > end_date)."
    )

    print("\nPipeline impact / decisions supported by this report:")
    print("- Deduplicate customers by cleaned email and keep earliest created_at.")
    print("- Exclude interactions whose customer_id is not present in customer_360.")
    print("- Null negative duration_seconds values.")
    print("- Flag purchase amount = 0 records with amount_flag = TRUE.")
    print("- Exclude untrusted source_system values from time-based analysis when they generate invalid/future timestamps.")
    print("- Preserve raw history whenever possible, but protect analytical features from distorted dates or broken keys.")

    print("\nRaw table sizes:")
    for table, count in row_counts.items():
        print(f"- {table}: {count}")


def generate_quality_report(con) -> None:
    """
    Gera um relatório de qualidade de dados enxuto, mas focado em decisão.

    A ideia aqui não é só listar problema, e sim mostrar o que impacta
    diretamente a limpeza, as regras do pipeline e a construção das features.
    """
    logger.info("Generating data quality report")

    print("\n" + "=" * 70)
    print("DATA QUALITY REPORT")
    print("=" * 70)

    row_counts = _print_row_counts(con)
    _print_key_nulls(con)
    ref_integrity = _print_referential_integrity(con)
    duplicates = _print_duplicate_email_analysis(con)
    customer_anomalies = _print_customer_anomalies(con)
    interaction_anomalies = _print_interaction_anomalies(con, row_counts["interactions"])
    transaction_anomalies = _print_transaction_anomalies(con)
    campaign_anomalies = _print_campaign_anomalies(con)

    _print_executive_summary(
        row_counts=row_counts,
        ref_integrity=ref_integrity,
        duplicates=duplicates,
        customer_anomalies=customer_anomalies,
        interaction_anomalies=interaction_anomalies,
        transaction_anomalies=transaction_anomalies,
        campaign_anomalies=campaign_anomalies,
    )

    print("\n" + "=" * 70)
    print("END OF DATA QUALITY REPORT")
    print("=" * 70 + "\n")

    logger.info("Data quality report finished successfully")