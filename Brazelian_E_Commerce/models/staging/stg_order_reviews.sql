{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('bronze_reviews', 'raw_order_reviews') }}
),

cleaned AS (
    SELECT
        --primary key
        review_id, 
        order_id,

        -- Review score should be between 1 and 5, if not, set to NULL
        CASE
            WHEN TRY_TO_DECIMAL(review_score) BETWEEN 1 AND 5 THEN TRY_TO_DECIMAL(review_score)
            ELSE NULL
        END AS review_score,

        --Remove whitespace from review_comment_title and review_comment_message, if NULL, set to 'No comment'
        COALESCE(TRIM(review_comment_title), 'No comment') AS review_comment_title,
        COALESCE(TRIM(review_comment_message), 'No comment') AS review_comment_message,

        TRY_TO_TIMESTAMP(review_creation_date) AS review_creation_date,
        TRY_TO_TIMESTAMP(review_answer_timestamp) AS review_answer_timestamp,

        -- Derived column: review_response_time = review_answer_timestamp - review_creation_date
        DATEDIFF(
            'day',
            TRY_TO_TIMESTAMP(review_creation_date),
            TRY_TO_TIMESTAMP(review_answer_timestamp)
        ) AS review_response_time,

        CASE
            WHEN TRY_TO_DECIMAL(review_score) >= 4 THEN TRUE
            ELSE FALSE
        END AS is_positive_review,
        
        -- Audit columns
        _file_name,
        _loaded_at

    FROM source
    WHERE review_id IS NOT NULL AND order_id IS NOT NULL 
)

SELECT * FROM cleaned