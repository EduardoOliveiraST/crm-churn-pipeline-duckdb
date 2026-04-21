-- Q1
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
        fp.first_purchase_date,
        MIN(t.transaction_ts) AS first_service_after_purchase_date
    FROM first_purchase fp
    JOIN clean_transactions t
        ON fp.customer_id = t.customer_id
    WHERE t.transaction_type = 'service'
      AND DATE_DIFF('day', fp.first_purchase_date, t.transaction_ts) BETWEEN 1 AND 365
    GROUP BY fp.customer_id, fp.first_purchase_date
)
SELECT
    customer_id,
    first_purchase_date,
    first_service_after_purchase_date
FROM service_after_purchase
ORDER BY customer_id;


-- Q2
WITH valid_campaign_interactions AS (
    SELECT
        c.channel,
        c.campaign_id,
        i.interaction_id,
        i.outcome
    FROM clean_interactions i
    JOIN clean_campaigns c
        ON i.campaign_id = c.campaign_id
    WHERE i.interaction_ts BETWEEN c.start_ts AND c.end_ts
),
aggregated AS (
    SELECT
        channel,
        COUNT(DISTINCT campaign_id) AS campaigns_in_channel,
        COUNT(*) AS total_linked_interactions,
        SUM(CASE WHEN outcome = 'interested' THEN 1 ELSE 0 END) AS positive_responses
    FROM valid_campaign_interactions
    GROUP BY channel
)
SELECT
    channel,
    campaigns_in_channel,
    total_linked_interactions,
    positive_responses,
    ROUND(
        100.0 * positive_responses / total_linked_interactions,
        2
    ) AS response_rate_pct
FROM aggregated
ORDER BY channel;

-- Q3
WITH last_interaction AS (
    SELECT
        customer_id,
        MAX(interaction_ts) AS last_interaction
    FROM clean_interactions
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.segment,
    CASE
        WHEN li.last_interaction IS NULL THEN NULL
        ELSE DATE_DIFF('day', li.last_interaction, DATE '2024-06-01')
    END AS days_since_last_interaction
FROM customer_360 c
LEFT JOIN last_interaction li
    ON c.customer_id = li.customer_id
WHERE li.last_interaction IS NULL
   OR DATE_DIFF('day', li.last_interaction, DATE '2024-06-01') > 180
ORDER BY days_since_last_interaction DESC NULLS LAST, c.customer_id;

-- Q4
WITH revenue_per_customer AS (
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
ranked_customers AS (
    SELECT
        c.state,
        c.customer_id,
        r.total_purchase_amount,
        DENSE_RANK() OVER (
            PARTITION BY c.state
            ORDER BY r.total_purchase_amount DESC
        ) AS rank_in_state
    FROM customer_360 c
    JOIN revenue_per_customer r
        ON c.customer_id = r.customer_id
    WHERE c.state IS NOT NULL
)
SELECT
    state,
    customer_id,
    total_purchase_amount,
    rank_in_state
FROM ranked_customers
WHERE rank_in_state <= 3
ORDER BY state, rank_in_state, customer_id;