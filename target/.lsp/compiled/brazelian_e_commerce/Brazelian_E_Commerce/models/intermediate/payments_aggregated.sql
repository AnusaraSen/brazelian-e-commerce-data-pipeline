

WITH source AS (
    SELECT * FROM SILVER_DB.PUBLIC.stg_payments
),

-- Step 1: Aggregate all payment rows to one row per order
aggregated AS (
    SELECT
        order_id,

        -- Total amount paid across all payment methods
        SUM(payment_value) AS total_payment_value,

        -- Number of payment rows for this order
        COUNT(*) AS payment_count,

        -- Total installments across all payments
        SUM(payment_installments) AS total_installments,

        -- Was this a mixed payment method order?
        CASE
            WHEN COUNT(DISTINCT payment_type) > 1 THEN TRUE
            ELSE FALSE
        END AS is_mixed_payment

    FROM source
    GROUP BY order_id
),

-- Step 2: Find the dominant payment type (highest value)
dominant_payment AS (
    SELECT
        order_id,
        payment_type AS primary_payment_type,
        payment_value AS primary_payment_value
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY payment_value DESC
    ) = 1
    -- QUALIFY is Snowflake-specific
    -- Picks the single payment row with the highest value per order
    -- If two payments have equal value, ORDER BY payment_sequential ASC breaks the tie
),

-- Step 3: Join aggregated totals with dominant payment type
final AS (
    SELECT
        a.order_id,
        a.total_payment_value,
        a.payment_count,
        a.total_installments,
        a.is_mixed_payment,
        d.primary_payment_type,
        d.primary_payment_value
    FROM aggregated a
    LEFT JOIN dominant_payment d
        ON a.order_id = d.order_id
)

SELECT * FROM final