{{ config(materialized='table') }}

WITH date_spine AS (
    {{
        dbt_utils.date_spine(
            datepart = "day",
            start_date = "cast('2016-01-01' as date)",
            end_date = "cast('2027-12-31' as date)"
        )
    }}
),

final AS (
    SELECT
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,
        date_day AS full_date,
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter_number,
        'Q' || QUARTER(date_day) AS quarter_name,
        MONTH(date_day) AS month_number,
        MONTHNAME(date_day) AS month_name,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        WEEKOFYEAR(date_day) AS week_of_year,
        DAY(date_day) AS day_of_month,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYNAME(date_day) AS day_name,

        -- Derived columns for weekend and weekday
        CASE
            WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        CASE
            WHEN DAYOFWEEK(date_day) IN (0, 6) THEN FALSE
            ELSE TRUE
        END AS is_weekday,

        -- Derived columns for today and past or today
        CASE
            WHEN date_day = CURRENT_DATE() THEN TRUE
            ELSE FALSE
        END AS is_today,

        CASE
            WHEN date_day <= CURRENT_DATE() THEN TRUE
            ELSE FALSE
        END AS is_past_or_today

    FROM date_spine
)

SELECT * FROM final