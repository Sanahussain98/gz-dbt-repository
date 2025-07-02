
SELECT
    date_date,
    SUM(ads_cost) AS daily_ads_cost,
    SUM(impression) AS daily_impressions,
    SUM(click) AS daily_clicks
FROM
    {{ ref('int_campaigns') }} -- Reference the combined campaigns model
GROUP BY
    date_date
ORDER BY
    date_date DESC -- Order by date in reverse chronological order
