{{ config(
    materialized='table' 
) }}

WITH daily_combined_data AS (
    SELECT
        f.date_date,
        f.nb_transactions,
        f.revenue,
        f.margin,
        f.operational_margin,
        f.purchase_cost,
        f.shipping_fee,
        f.log_cost,
        f.ship_cost,
        f.quantity,
        c.daily_ads_cost,
        c.daily_impressions,
        c.daily_clicks
    FROM
        {{ ref('finance_days') }} AS f
    LEFT JOIN
        {{ ref('int_campaigns_day') }} AS c
    ON
        f.date_date = c.date_date
)

SELECT
    DATE_TRUNC(date_date, MONTH) AS datemonth, -- Aggregate to month
    SUM(operational_margin - daily_ads_cost) AS ads_margin,
    ROUND(SUM(revenue) / NULLIF(SUM(nb_transactions), 0), 2) AS average_basket, -- Recalculate monthly average basket
    SUM(operational_margin) AS operational_margin,
    SUM(daily_ads_cost) AS ads_cost,
    SUM(daily_impressions) AS ads_impression,
    SUM(daily_clicks) AS ads_clicks,
    SUM(quantity) AS quantity,
    SUM(revenue) AS revenue,
    SUM(purchase_cost) AS purchase_cost,
    SUM(margin) AS margin,
    SUM(shipping_fee) AS shipping_fee,
    SUM(log_cost) AS log_cost,
    SUM(ship_cost) AS ship_cost
FROM
    daily_combined_data
GROUP BY
    datemonth
ORDER BY
    datemonth DESC -- Order by month in reverse chronological order
