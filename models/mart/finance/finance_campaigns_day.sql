 ---his mart model combines daily financial data with daily ad campaign data.
-- It calculates 'ads_margin' and presents key metrics for the finance team.

{{ config(
    materialized='view' 
) }}

SELECT
    f.date_date AS date, -- Renamed to 'date' as requested
    f.operational_margin - c.daily_ads_cost AS ads_margin, -- Calculate ads_margin
    f.average_basket,
    f.operational_margin,
    c.daily_ads_cost AS ads_cost, -- Renamed to 'ads_cost' from 'daily_ads_cost'
    c.daily_impressions AS ads_impression, -- Renamed to 'ads_impression'
    c.daily_clicks AS ads_clicks, -- Renamed to 'ads_clicks'
    f.quantity,
    f.revenue,
    f.purchase_cost,
    f.margin,
    f.shipping_fee,
    f.log_cost,
    f.ship_cost
FROM
    {{ ref('finance_days') }} AS f -- Reference the finance_days model
LEFT JOIN
    {{ ref('int_campaigns_day') }} AS c -- Reference the int_campaigns_day model
ON
    f.date_date = c.date_date -- Join on the common date column
ORDER BY
    f.date_date DESC -- Order by date in reverse chronological order
