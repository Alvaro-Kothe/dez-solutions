WITH quarterly_revenue AS (
    SELECT
        service_type,
        year_quarter,
        year,
        quarter,
        SUM(total_amount) AS revenue
    FROM {{ ref("dim_taxi_trips") }}
    GROUP BY service_type, year_quarter, year, quarter
),

revenue_with_lag AS (
    SELECT
        service_type,
        year_quarter,
        revenue,
        LAG(revenue, 1)
            OVER (
                PARTITION BY service_type, quarter
                ORDER BY year
            )
            AS previous_year_revenue
    FROM quarterly_revenue
)

SELECT
    service_type,
    year_quarter,
    revenue,
    previous_year_revenue,
    CASE
        WHEN previous_year_revenue > 0
            THEN
                (
                    (revenue - previous_year_revenue)
                    / previous_year_revenue
                )
    END AS yoy
FROM revenue_with_lag
ORDER BY service_type, year_quarter
