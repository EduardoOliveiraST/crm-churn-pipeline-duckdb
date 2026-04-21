from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Data de referência usada depois nas features (não usada no cleaning)
REFERENCE_DATE = "2024-06-01"


def build_customer_360(con) -> None:
    """
    Monta a tabela customer_360 (visão canônica de cliente).

    Regras principais:
    - Deduplicação por email tratado (lower + trim)
    - Mantém o registro mais antigo (created_at mais cedo)
    - Marca se o email apareceu mais de uma vez na base original
    """
    logger.info("Building customer_360")

    query = """
    CREATE OR REPLACE TABLE customer_360 AS
    WITH cleaned AS (
        SELECT
            customer_id,

            -- Padronização de email (base da deduplicação)
            LOWER(TRIM(email)) AS email,

            -- Limpeza de telefone:
            -- 11 dígitos → mantém
            -- 10 dígitos → adiciona 9 após DDD (padrão BR)
            -- resto → descarta
            CASE
                WHEN LENGTH(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g')) = 11 THEN
                    REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g')

                WHEN LENGTH(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g')) = 10 THEN
                    SUBSTR(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g'), 1, 2)
                    || '9' ||
                    SUBSTR(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g'), 3)

                ELSE NULL
            END AS phone,

            -- Cast seguro das datas
            TRY_CAST(birth_date AS DATE) AS birth_date,
            city,
            state,
            segment,
            salesforce_id,
            TRY_CAST(created_at AS TIMESTAMP) AS created_at
        FROM customers
        WHERE email IS NOT NULL
          AND TRIM(email) <> ''
    ),

    dedup AS (
        SELECT
            *,

            -- Flag se esse email aparece mais de uma vez
            COUNT(*) OVER (PARTITION BY email) > 1 AS is_duplicate_email,

            -- Define ranking pra deduplicação
            ROW_NUMBER() OVER (
                PARTITION BY email
                ORDER BY created_at ASC NULLS LAST, customer_id
            ) AS rn
        FROM cleaned
    )

    -- Mantém só o melhor registro por email
    SELECT
        customer_id,
        email,
        phone,
        birth_date,
        city,
        state,
        segment,
        salesforce_id,
        created_at,
        is_duplicate_email
    FROM dedup
    WHERE rn = 1
    """

    con.execute(query)
    logger.info("customer_360 built successfully")


def clean_interactions(con) -> None:
    """
    Monta a tabela clean_interactions.

    Regras aplicadas:
    - Mantém só interações com customer válido (inner join com customer_360)
    - Faz parse seguro da data (interaction_ts)
    - duration negativa vira NULL (erro de origem)
    - Remove source_system não confiável (LEGACY_V2)

    Observação importante:
    - NÃO filtra por data de referência aqui
      → cleaning preserva histórico
      → cortes temporais ficam pra camada analítica
    """
    logger.info("Cleaning interactions")

    query = """
    CREATE OR REPLACE TABLE clean_interactions AS
    WITH base AS (
        SELECT
            i.*,

            -- Parse da data original
            TRY_CAST(i.interaction_date AS TIMESTAMP) AS interaction_ts
        FROM interactions i
    )
    SELECT
        b.interaction_id,
        b.customer_id,
        b.interaction_date,
        b.interaction_ts,
        b.interaction_type,

        -- Corrige duração inválida
        CASE
            WHEN b.duration_seconds < 0 THEN NULL
            ELSE b.duration_seconds
        END AS duration_seconds,

        b.channel,
        b.outcome,
        b.campaign_id,
        b.source_system
    FROM base b

    -- Garante consistência com master de clientes
    INNER JOIN customer_360 c
        ON b.customer_id = c.customer_id

    WHERE b.interaction_ts IS NOT NULL

      -- Remove fonte problemática identificada na exploração
      AND COALESCE(TRIM(b.source_system), '') <> 'LEGACY_V2'
    """

    con.execute(query)
    logger.info("clean_interactions built successfully")


def clean_transactions(con) -> None:
    """
    Monta a tabela clean_transactions.

    Regras:
    - Não remove registros (exigência do exercício)
    - Adiciona transaction_ts pra facilitar análises temporais
    - Marca compras com amount = 0 (não descarta, apenas sinaliza)
    """
    logger.info("Cleaning transactions")

    query = """
    CREATE OR REPLACE TABLE clean_transactions AS
    WITH base AS (
        SELECT
            t.*,

            -- Parse da data
            TRY_CAST(t.transaction_date AS TIMESTAMP) AS transaction_ts
        FROM transactions t
    )
    SELECT
        transaction_id,
        customer_id,
        transaction_type,
        amount,
        transaction_date,
        transaction_ts,
        dealership_id,

        -- Flag de inconsistência em compras
        CASE
            WHEN transaction_type = 'purchase' AND amount = 0 THEN TRUE
            ELSE FALSE
        END AS amount_flag
    FROM base
    """

    con.execute(query)
    logger.info("clean_transactions built successfully")


def clean_campaigns(con) -> None:
    """
    Monta a tabela clean_campaigns (base auxiliar).

    Regras:
    - Faz parse das datas
    - Mantém apenas campanhas com janela válida (start <= end)
    """
    logger.info("Cleaning campaigns")

    query = """
    CREATE OR REPLACE TABLE clean_campaigns AS
    WITH base AS (
        SELECT
            c.*,

            -- Parse seguro das datas
            TRY_CAST(c.start_date AS TIMESTAMP) AS start_ts,
            TRY_CAST(c.end_date AS TIMESTAMP) AS end_ts
        FROM campaigns c
    )
    SELECT
        campaign_id,
        campaign_name,
        channel,
        target_segment,
        start_date,
        end_date,
        start_ts,
        end_ts
    FROM base
    WHERE start_ts IS NOT NULL
      AND end_ts IS NOT NULL

      -- Garante coerência temporal da campanha
      AND start_ts <= end_ts
    """

    con.execute(query)
    logger.info("clean_campaigns built successfully")