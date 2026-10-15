# Looker Studio dashboard, reviewer sharing and cost report screenshots

Manual steps: Looker Studio and screenshots cannot be created from this repository. Do them after at least one successful `ride_hailing_daily` run, so the views have data.

## 1. Data sources (connect each as a separate BigQuery source, project `labs-507309`, dataset `mart_ds`)
| Source | View | Used for |
|---|---|---|
| Daily revenue | `v_daily_revenue` | KPI tiles and the 30-day line chart |
| Hourly demand | `v_zone_demand_hourly` | heatmap |
| Forecast | `v_zone_demand_forecast` | forecast overlay and table |
| Drivers | `v_driver_leaderboard` | leaderboard table |
Use "Owner's credentials" for the data source so reviewers do not need BigQuery access. Cache and a date-range control keep query cost low.

## 2. Pages and charts (matching the brief)
| Element | Source and setup |
|---|---|
| KPI: yesterday's trips | `v_daily_revenue`, metric `rides` (SUM), date = yesterday (default date range: yesterday), no `source` filter |
| KPI: revenue | metric `revenue` |
| KPI: avg surge | metric `surge_ride_share` is the share of surged rides; for the average multiplier add the field `surge_multiplier` in `fact_rides` as a custom source if you want it (the view exposes the share, not the mean) |
| KPI: active drivers | metric `active_drivers` (stream rides only; historical rides have no driver) |
| Line chart: revenue, last 30 days | dimension `pickup_date`, metric `revenue`, date range last 30 days, breakdown by `source` |
| Heatmap: hourly demand by zone | pivot table with heatmap: rows `pickup_zone_name` (or `pickup_zone`), columns hour of `hour_ts`, metric `rides` |
| Forecast overlay | time series on `v_zone_demand_forecast`: dimension `forecast_ts`, metrics `predicted_rides`, `lower_bound`, `upper_bound`, `actual_rides`, with a zone filter control on `pickup_zone_name` |
| Leaderboard | table of `v_driver_leaderboard` sorted by `revenue_rank` |
Note: the 2018 history and the current stream are different periods. For demo data, set the date range to the period you loaded and say so on screen.

## 3. Reviewer-sharing checklist
- [ ] Report > Share > "Anyone with the link can view" OR add each reviewer's e-mail as Viewer.
- [ ] Data source credentials are "Owner's credentials" (otherwise reviewers see permission errors).
- [ ] Open the link in a private/incognito window to confirm it loads without your login.
- [ ] No page shows raw driver or rider identifiers that should not be public (`rider_id` is not in any mart; driver ids are pseudonymous).
- [ ] Put the link in the README under "Looker Studio dashboard" and in the demo notes.
- [ ] Optional: export a PDF of the report as a fallback.

## 4. Cost report screenshot procedure
1. In the BigQuery console set the project and location `asia-south1`. The identity needs `bigquery.jobs.listAll` (for example `roles/bigquery.resourceViewer`).
2. Open `sql/cost/cost_daily_bytes_billed.sql`, add the parameters `days` (INT64, 30) and `price_per_tib` (FLOAT64, the current on-demand price from the pricing page) under Query settings > Query parameters, run it. Screenshot 1: the result table (daily bytes billed, estimated cost).
3. Run `sql/cost/cost_expensive_queries.sql` with `days` 30, `top_n` 10, the same price. Screenshot 2: the top queries.
4. Optional: `sql/cost/cost_budget_check.sql` with `budget_usd` (50) for the month-to-date status.
5. Save screenshots in `docs/evidence/` (for example `cost-daily-bytes.png`, `cost-top-queries.png`) with the date in the name. Do not crop out the project id and the time stamps.
6. In the demo, state: what the cap is (10 GB per query), which query was the most expensive and why (for example the forecast training series scanning the view), and what you changed or would change (partition filters, caching, a narrower view).
The estimate ignores free-tier credits and BQML pricing that is not byte-based; say so.
