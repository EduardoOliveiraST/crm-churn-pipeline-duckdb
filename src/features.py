from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Data de referência fixa usada pra todos os cálculos de tempo
REFERENCE_DATE = "2024-06-01"


def build_churn_features(con) -> None:
    """
    Monta a tabela churn_features (1 linha por cliente).

    Tudo aqui é calculado com base na data de referência fixa (2024-06-01),
    garantindo consistência entre todas as métricas temporais.
    """
    logger.info("Building churn_features")

    query = f"""
    CREATE OR REPLACE TABLE churn_features AS

    -- Base de clientes (garante 1 linha por customer_id no final)
    WITH base AS (
        SELECT customer_id
        FROM customer_360
    ),

    -- Agregações de interações (engajamento do cliente)
    interactions_agg AS (
        SELECT
            customer_id,

            -- Última interação até a data de referência
            MAX(interaction_ts) FILTER (
                WHERE interaction_ts <= DATE '{REFERENCE_DATE}'
            ) AS last_interaction,

            -- Quantidade de interações nos últimos 90 dias
            COUNT(*) FILTER (
                WHERE interaction_ts BETWEEN
                    DATE '{REFERENCE_DATE}' - INTERVAL 90 DAY
                    AND DATE '{REFERENCE_DATE}'
            ) AS interaction_count_90d
        FROM clean_interactions
        GROUP BY customer_id
    ),

    -- Agregações de transações (compras e receita)
    transactions_agg AS (
        SELECT
            customer_id,

            -- Total de compras válidas até a data de referência
            COUNT(*) FILTER (
                WHERE transaction_type = 'purchase'
                  AND transaction_ts <= DATE '{REFERENCE_DATE}'
            ) AS purchase_count,

            -- Data da última compra
            MAX(transaction_ts) FILTER (
                WHERE transaction_type = 'purchase'
                  AND transaction_ts <= DATE '{REFERENCE_DATE}'
            ) AS last_purchase,

            -- Receita total (filtrando valores inválidos e negativos)
            SUM(
                CASE
                    WHEN transaction_type = 'purchase'
                     AND transaction_ts <= DATE '{REFERENCE_DATE}'
                     AND COALESCE(amount_flag, FALSE) = FALSE
                     AND amount > 0
                    THEN amount
                    ELSE 0
                END
            ) AS total_revenue,

            -- Flag indicando se já fez test drive
            MAX(
                CASE
                    WHEN transaction_type = 'test_drive'
                     AND transaction_ts <= DATE '{REFERENCE_DATE}'
                    THEN 1
                    ELSE 0
                END
            ) AS has_test_drive
        FROM clean_transactions
        GROUP BY customer_id
    ),

    -- Eventos de serviço (usados pra calcular intervalo médio)
    service_events AS (
        SELECT
            customer_id,
            transaction_ts
        FROM clean_transactions
        WHERE transaction_type = 'service'
          AND transaction_ts IS NOT NULL
          AND transaction_ts <= DATE '{REFERENCE_DATE}'
    ),

    -- Intervalo médio entre serviços (diferença entre eventos consecutivos)
    service_intervals AS (
        SELECT
            customer_id,
            AVG(diff_days) AS avg_days_between_services
        FROM (
            SELECT
                customer_id,
                DATE_DIFF(
                    'day',
                    LAG(transaction_ts) OVER (
                        PARTITION BY customer_id
                        ORDER BY transaction_ts
                    ),
                    transaction_ts
                ) AS diff_days
            FROM service_events
        ) s
        WHERE diff_days IS NOT NULL
        GROUP BY customer_id
    ),

    -- Deduplicação de campanhas (evita duplicidade no join)
    campaign_dedup AS (
        SELECT DISTINCT
            campaign_id,
            channel,
            start_ts,
            end_ts
        FROM clean_campaigns
    ),

    -- Interações que aconteceram durante campanhas
    campaign_interactions AS (
        SELECT
            i.customer_id,
            i.outcome
        FROM clean_interactions i
        INNER JOIN campaign_dedup c
            ON i.campaign_id = c.campaign_id
        WHERE i.interaction_ts BETWEEN c.start_ts AND c.end_ts
          AND i.interaction_ts <= DATE '{REFERENCE_DATE}'
    ),

    -- Métricas de resposta a campanhas
    campaign_agg AS (
        SELECT
            customer_id,

            -- Total de interações com campanhas
            COUNT(*) AS total_campaign_interactions,

            -- Quantas foram positivas (interessado)
            SUM(CASE WHEN outcome = 'interested' THEN 1 ELSE 0 END) AS positive_responses,

            -- Taxa de resposta (evita divisão por zero)
            CASE
                WHEN COUNT(*) > 0 THEN
                    ROUND(
                        100.0 * SUM(CASE WHEN outcome = 'interested' THEN 1 ELSE 0 END) / COUNT(*),
                        2
                    )
                ELSE NULL
            END AS campaign_response_rate
        FROM campaign_interactions
        GROUP BY customer_id
    )

    -- Montagem final da tabela de features
    SELECT
        b.customer_id,

        -- Dias desde a última interação
        CASE
            WHEN i.last_interaction IS NOT NULL THEN
                DATE_DIFF('day', i.last_interaction, DATE '{REFERENCE_DATE}')
            ELSE NULL
        END AS recency_days,

        COALESCE(i.interaction_count_90d, 0) AS interaction_count_90d,

        COALESCE(t.purchase_count, 0) AS purchase_count,

        -- Dias desde a última compra
        CASE
            WHEN t.last_purchase IS NOT NULL THEN
                DATE_DIFF('day', t.last_purchase, DATE '{REFERENCE_DATE}')
            ELSE NULL
        END AS days_since_last_purchase,

        COALESCE(t.total_revenue, 0) AS total_revenue,

        c.campaign_response_rate,

        COALESCE(t.has_test_drive, 0) AS has_test_drive,

        s.avg_days_between_services

    FROM base b
    LEFT JOIN interactions_agg i USING (customer_id)
    LEFT JOIN transactions_agg t USING (customer_id)
    LEFT JOIN service_intervals s USING (customer_id)
    LEFT JOIN campaign_agg c USING (customer_id)
    """

    con.execute(query)
    logger.info("churn_features built successfully")
