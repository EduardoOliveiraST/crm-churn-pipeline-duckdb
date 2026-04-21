import duckdb

# conexão local simples (pipeline já gerou os CSVs)
con = duckdb.connect()

print("\n📦 LOADING OUTPUT DATASETS...\n")

# carrego os outputs finais gerados pelo pipeline
con.execute("""
CREATE OR REPLACE TABLE customer_360 AS
SELECT * FROM read_csv_auto('output/customer_360.csv', HEADER=TRUE);
""")

con.execute("""
CREATE OR REPLACE TABLE clean_interactions AS
SELECT * FROM read_csv_auto('output/clean_interactions.csv', HEADER=TRUE);
""")

con.execute("""
CREATE OR REPLACE TABLE clean_transactions AS
SELECT * FROM read_csv_auto('output/clean_transactions.csv', HEADER=TRUE);
""")

con.execute("""
CREATE OR REPLACE TABLE churn_features AS
SELECT * FROM read_csv_auto('output/churn_features.csv', HEADER=TRUE);
""")

# importante: aqui eu uso o dataset já limpo (não o raw)
# isso garante consistência com o pipeline principal
con.execute("""
CREATE OR REPLACE TABLE clean_campaigns AS
SELECT * FROM read_csv_auto('output/clean_campaigns.csv', HEADER=TRUE);
""")

print("✅ Data loaded successfully\n")


# =========================================================
# 📊 BASIC STATS
# =========================================================
print("📊 ROW COUNTS")
print(con.execute("""
SELECT
    (SELECT COUNT(*) FROM customer_360) AS customer_360,
    (SELECT COUNT(*) FROM clean_interactions) AS clean_interactions,
    (SELECT COUNT(*) FROM clean_transactions) AS clean_transactions,
    (SELECT COUNT(*) FROM churn_features) AS churn_features,
    (SELECT COUNT(*) FROM clean_campaigns) AS clean_campaigns
""").fetchdf())


# =========================================================
# 👀 SAMPLE DATA
# =========================================================
print("\n👤 SAMPLE customer_360")
print(con.execute("SELECT * FROM customer_360 LIMIT 5").fetchdf())

# checo quantos telefones não consegui resolver
print("\n📞 PHONE NULL COUNT")
print(con.execute("""
SELECT COUNT(*) AS phone_nulls
FROM customer_360
WHERE phone IS NULL
""").fetchdf())


print("\n📡 SAMPLE clean_interactions")
print(con.execute("SELECT * FROM clean_interactions LIMIT 5").fetchdf())

# garantia: nenhuma duração negativa passou pela limpeza
print("\n🧪 NEGATIVE DURATION CHECK")
print(con.execute("""
SELECT COUNT(*) AS negative_duration
FROM clean_interactions
WHERE duration_seconds < 0
""").fetchdf())

# garantia: nenhuma data futura está contaminando análise
print("\n🚨 FUTURE DATES CHECK")
print(con.execute("""
SELECT COUNT(*) AS future_dates
FROM clean_interactions
WHERE interaction_ts > TIMESTAMP '2024-06-01'
""").fetchdf())


print("\n💰 SAMPLE clean_transactions")
print(con.execute("SELECT * FROM clean_transactions LIMIT 5").fetchdf())

# sanity check: compras com valor zero continuam presentes (não foram dropadas)
print("\n🚨 ZERO AMOUNT PURCHASES")
print(con.execute("""
SELECT COUNT(*) AS zero_purchase
FROM clean_transactions
WHERE transaction_type = 'purchase' AND amount = 0
""").fetchdf())


print("\n📣 SAMPLE clean_campaigns")
print(con.execute("SELECT * FROM clean_campaigns LIMIT 5").fetchdf())

# garantia: campanhas inválidas não passaram
print("\n🚨 INVALID CAMPAIGN WINDOWS")
print(con.execute("""
SELECT COUNT(*) AS invalid_campaign_windows
FROM clean_campaigns
WHERE TRY_CAST(start_date AS TIMESTAMP) > TRY_CAST(end_date AS TIMESTAMP)
""").fetchdf())


# =========================================================
# 🧠 FEATURE VALIDATION
# =========================================================
print("\n🧠 SAMPLE churn_features")
print(con.execute("SELECT * FROM churn_features LIMIT 5").fetchdf())

# visão geral das features (range ajuda a detectar anomalia)
print("\n📊 FEATURE STATS")
print(con.execute("""
SELECT
    COUNT(*) AS total_customers,
    MIN(recency_days) AS min_recency,
    MAX(recency_days) AS max_recency,
    MIN(purchase_count) AS min_purchase,
    MAX(purchase_count) AS max_purchase,
    MIN(total_revenue) AS min_revenue,
    MAX(total_revenue) AS max_revenue
FROM churn_features
""").fetchdf())


# =========================================================
# 🔥 Q1 VALIDATION
# =========================================================
print("\n🔥 Q1 RESULT COUNT")
print(con.execute("""
-- clientes com service até 365 dias após primeira compra
WITH first_purchase AS (
    SELECT
        customer_id,
        MIN(transaction_ts) AS first_purchase_date
    FROM clean_transactions
    WHERE transaction_type = 'purchase'
    GROUP BY customer_id
),
service_after_purchase AS (
    SELECT
        fp.customer_id,
        MIN(t.transaction_ts) AS first_service_after_purchase_date
    FROM first_purchase fp
    JOIN clean_transactions t
        ON fp.customer_id = t.customer_id
    WHERE t.transaction_type = 'service'
      AND DATE_DIFF('day', fp.first_purchase_date, t.transaction_ts) BETWEEN 1 AND 365
    GROUP BY fp.customer_id
)
SELECT COUNT(*) AS q1_count
FROM service_after_purchase;
""").fetchdf())


# =========================================================
# 🔥 Q2 VALIDATION
# =========================================================
print("\n🔥 Q2 RESULT BY CHANNEL")
print(con.execute("""
-- aqui valido exatamente o que o enunciado pede:
-- campanhas + interações + respostas positivas por canal
WITH valid_campaign_interactions AS (
    SELECT
        c.channel,
        c.campaign_id,
        i.interaction_id,
        i.outcome
    FROM clean_campaigns c
    LEFT JOIN clean_interactions i
        ON c.campaign_id = i.campaign_id
       AND i.interaction_ts BETWEEN TRY_CAST(c.start_date AS TIMESTAMP)
                               AND TRY_CAST(c.end_date AS TIMESTAMP)
)
SELECT
    channel,
    COUNT(DISTINCT campaign_id) AS campaigns_in_channel,
    COUNT(interaction_id) AS total_linked_interactions,
    SUM(CASE WHEN outcome = 'interested' THEN 1 ELSE 0 END) AS positive_responses,
    ROUND(
        100.0 * SUM(CASE WHEN outcome = 'interested' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(interaction_id), 0),
        2
    ) AS response_rate_pct
FROM valid_campaign_interactions
GROUP BY channel
ORDER BY channel;
""").fetchdf())


# =========================================================
# 🔥 Q3 VALIDATION
# =========================================================
print("\n🔥 Q3 INACTIVE CUSTOMERS")
print(con.execute("""
-- importante: uso LEFT JOIN pra incluir clientes sem interação
-- isso garante visão completa de churn (inclusive clientes "silenciosos")
WITH last_interaction AS (
    SELECT
        customer_id,
        MAX(interaction_ts) AS last_interaction
    FROM clean_interactions
    GROUP BY customer_id
)
SELECT COUNT(*) AS q3_count
FROM customer_360 c
LEFT JOIN last_interaction li
    USING(customer_id)
WHERE li.last_interaction IS NULL
   OR DATE_DIFF('day', li.last_interaction, DATE '2024-06-01') > 180;
""").fetchdf())


# =========================================================
# 🔥 Q4 VALIDATION
# =========================================================
print("\n🔥 Q4 TOP CUSTOMERS PER STATE")
print(con.execute("""
-- uso ROW_NUMBER pra garantir exatamente top 3 por estado
WITH revenue AS (
    SELECT
        customer_id,
        SUM(
            CASE
                WHEN transaction_type = 'purchase' AND amount > 0 THEN amount
                ELSE 0
            END
        ) AS total_purchase_amount
    FROM clean_transactions
    GROUP BY customer_id
),
ranked AS (
    SELECT
        c.state,
        c.customer_id,
        r.total_purchase_amount,
        ROW_NUMBER() OVER (
            PARTITION BY c.state
            ORDER BY r.total_purchase_amount DESC, c.customer_id
        ) AS rank_in_state
    FROM customer_360 c
    JOIN revenue r
        USING(customer_id)
    WHERE c.state IS NOT NULL
)
SELECT COUNT(*) AS q4_count
FROM ranked
WHERE rank_in_state <= 3;
""").fetchdf())


print("\n✅ INSPECTION COMPLETE\n")