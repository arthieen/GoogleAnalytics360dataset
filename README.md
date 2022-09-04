# GoogleAnalytics360dataset
Google Analytics sample dataset for BigQuery

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT FORMAT_DATE("%Y%m",parse_DATE("%Y%m%d", date)) as month
--  ,count(visitId)
  ,sum( totals.visits) as visits
  ,sum(totals.pageviews ) as pageviews
  ,sum(totals.transactions ) as transactions
  ,sum(totals.totalTransactionRevenue )/1000000 as Revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
group by month
order by month

 
 -- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
 SELECT trafficSource.source as source
  , sum(totals.visits) as total_visits
  , sum(totals.bounces) as total_no_of_bounces
  , sum(totals.bounces)/sum(totals.visits)*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by trafficSource.source
order by total_visits desc
 


-- Query 03: Revenue by traffic source by week, by month in June 2017
#standardSQL
WITH cte_month AS(
  SELECT  
    case when date is not null then 'Month' end AS time_type,
    FORMAT_DATE("%Y%m",parse_DATE("%Y%m%d", date)) as time
    ,trafficSource.source as source
    , sum(totals.totalTransactionRevenue )/1000000 as Revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  group by time,trafficSource.source ,time_type
  having sum(totals.totalTransactionRevenue )/1000000 IS NOT NULL
  ),cte_week as
  (SELECT  
    case when date is not null then 'Week' end AS time_type,
    FORMAT_DATE("%Y%W",parse_DATE("%Y%m%d", date)) as time
    ,trafficSource.source as source
    , sum(totals.totalTransactionRevenue )/1000000 as Revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  group by time,trafficSource.source ,time_type
  having sum(totals.totalTransactionRevenue )/1000000 IS NOT NULL
  )
SELECT * FROM cte_month 
union all
SELECT * FROM cte_week 
order by 4 desc
--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
 
WITH cte_purchaser AS(
SELECT      
    FORMAT_DATE("%Y%m",parse_DATE("%Y%m%d", date)) as month
    , sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _TABLE_SUFFIX between '20170601' and '20170731' 
  and totals.transactions >=1
group by month)
, cte_nonpurchaser AS(
SELECT      
    FORMAT_DATE("%Y%m",parse_DATE("%Y%m%d", date)) as month
    , sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_non_purchase 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _TABLE_SUFFIX between '20170601' and '20170731' 
   and totals.transactions is null
 group by month)
SELECT * FROM cte_purchaser 
JOIN cte_nonpurchaser 
using(month)
ORDER by month
 
 
 
-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
 SELECT      
    FORMAT_DATE("%Y%m",parse_DATE("%Y%m%d", date)) as month
    , sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _TABLE_SUFFIX like '201707%'
  and totals.transactions >=1
group by month



-- Query 06: Average amount of money spent per session
#standardSQL
SELECT      
    FORMAT_DATE("%Y%m",parse_DATE("%Y%m%d", date)) as month
    , FORMAT("%'.2f",sum(totals.totalTransactionRevenue)/count(distinct visitId)) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _TABLE_SUFFIX like '201707%'
  and totals.transactions IS NOT NULL
group by month
 

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL   
WITH customer_YMVH_201707 AS(
  SELECT      
      visitId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    ,UNNEST (hits) hits
    ,UNNEST (hits.product) product
  where product.v2ProductName="YouTube Men's Vintage Henley"
    and _TABLE_SUFFIX like '201707%'
    and product.productRevenue IS NOT NULL
)
SELECT      
    product.v2ProductName
    , sum( product.productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  ,UNNEST (hits) hits
  ,UNNEST (hits.product) product
where product.v2ProductName<>"YouTube Men's Vintage Henley"
  and visitId in (select visitId from customer_YMVH_201707 )
  and _TABLE_SUFFIX like '201707%'
  and product.productRevenue IS NOT NULL
group by product.v2ProductName
order by quantity desc
-- Query 08 : Calculate from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)
 
select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data
