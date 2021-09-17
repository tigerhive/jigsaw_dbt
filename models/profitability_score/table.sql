{{ config(
    materialized="incremental",
    unique_key = "id",
    alias='profitability_score',
    merge_update_columns = [
                "outflows_90_avg_shift_90",
                "outflows_90_sum_shift_90",
                "outflows_35_avg",
                "outflows_28_sum",
                "outflows_35_sum",
                "outflows_28_avg",
                "profitability_90_avg_shift_90",
                "profitability_90_sum_shift_90",
                "revenue_35_sum_shift_90",
                "revenue_35_avg_shift_90",
                "transactions_90_sum_shift_90",
                "transactions_90_avg_shift_90",
                "revenue_90_sum_shift_28",
                "revenue_90_avg_shift_28",
                "inflows_28_avg",
                "inflows_28_sum",
                "inflows_35_sum",
                "inflows_35_avg",
                "transactions_14_avg",
                "transactions_28_avg",
                "transactions_28_sum",
                "transactions_35_avg",
                "transactions_35_sum",
                "revenue_28_avg_shift_90",
                "revenue_28_sum_shift_90",
                "revenue_90_sum_shift_90",
                "revenue_90_avg_shift_90",
                "profitability_90_avg",
                "profitability_90_sum",
                "inflows_90_avg",
                "inflows_90_sum",
                "outflows_90_avg",
                "outflows_90_sum",
                "transactions_90_avg",
                "transactions_90_sum",
                "inflows_90_sum_shift_90",
                "inflows_90_avg_shift_90",
                "revenue_14_sum",
                "revenue_28_sum",
                "revenue_35_sum",
                "revenue_90_sum",
                "revenue_14_avg",
                "revenue_28_avg",
                "revenue_35_avg",
                "revenue_90_avg",
                "prof_score",
                "run_id",
                "warehouse_update_date"],
    query_tag = "profitability_score"
) }}

with trx as (
   select
        T.partner_guid
        , transaction_date
        , (substr(transaction_date::string, 0, 4) || '-' || substr(transaction_date::string, 5, 2) || '-' || substr(transaction_date::string, 7, 2))::date as date
        , sum(transaction_amount) as ebitda
        , sum(case when transaction_amount > 0 then transaction_amount else 0 end) as revenue
        , ebitda/nullifzero(revenue) as profitability
        , count(transaction_date) as transactions
        , count(case when transaction_amount > 0 then transaction_date else null end) as inflows
        , count(case when transaction_amount < 0 then transaction_date else null end) as outflows

   
   from temp.transactions T
  join temp.addresses A on A.partner_guid = T.partner_guid
  where 1=1
    and (substr(period::string, 0, 4) || '-' 
            || substr(period::string, 5, 2) || - '01')::date between dateadd(days, -215, current_date) and current_date 
    
  group by 1, 2, 3),
  
  unique_companies as (
 select distinct partner_guid from trx
) ,
    
dates as (
   select distinct D.date, U.partner_guid
   
   from  raw.dates D
  cross join unique_companies U
   
   where d.date between dateadd(days, -181, current_date) and current_date
 
    order by 1
 )
  ,
  
  dates_frame as (
  --7, 14, 28, 35, 90
  select
    D.*
    
    , coalesce(trx.ebitda, 0) as ebitda
    , coalesce(trx.revenue, 0) as revenue
    , coalesce(trx.profitability, 0) as profitability
    , coalesce(trx.transactions, 0) as transactions
    , coalesce(trx.inflows, 0) as inflows
    , coalesce(trx.outflows, 0) as outflows
    
      
  from dates D
  left join trx on D.date = trx.date and d.partner_guid = TRX.partner_guid
  
  where
    1=1
    and d.date between dateadd(days, -181, current_date) and current_date
    --and trx.partner_guid is not null
    --and partner_guid = '4553FC77E061010D000000000AB6F52B'
    qualify sum(transactions) over (partition by D.partner_guid order by d.date asc ROWS BETWEEN 91 PRECEDING AND 1 PRECEDING ) > 0
  order by 1
    ),
   aggregated as (
    select 
      md5(CURRENT_DATE||'-'||D.partner_guid)::VARCHAR(32) AS ID,
       CURRENT_DATE::DATE AS date_TD
     , D.partner_guid::VARCHAR(32) AS PARTNER_ID
     
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then outflows else null end) / nullif(avg(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then outflows else null end), 0) as outflows_90_AVG_SHIFT_90
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then outflows else null end) / nullif(sum(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then outflows else null end), 0) as outflows_90_SUM_SHIFT_90
     
     , avg(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then outflows else null end) as outflows_35_avg     
     , sum(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then outflows else null end) as outflows_28_sum
     , sum(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then outflows else null end) as outflows_35_SUM
     , avg(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then outflows else null end) as outflows_28_avg
     
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then profitability else null end) / nullif(avg(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then profitability else null end), 0) as profitability_90_AVG_SHIFT_90
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then profitability else null end) / nullif(sum(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then profitability else null end), 0) as profitability_90_SUM_SHIFT_90

     , sum(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then revenue else null end) / nullif(sum(case when d.date between dateadd(days, -126, current_date) and dateadd(days, -92, current_date) then revenue else null end), 0) as revenue_35_SUM_SHIFT_90
     , avg(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then revenue else null end) / nullif(avg(case when d.date between dateadd(days, -126, current_date) and dateadd(days, -92, current_date) then revenue else null end), 0) as revenue_35_AVG_SHIFT_90

     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then transactions else null end) / nullif(sum(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then transactions else null end), 0) as transactions_90_SUM_SHIFT_90
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then transactions else null end) / nullif(avg(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then transactions else null end), 0) as transactions_90_avg_SHIFT_90
     
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then revenue else null end) / nullif(sum(case when d.date between dateadd(days, -119, current_date) and dateadd(days, -30, current_date) then revenue else null end), 0) as revenue_90_SUM_SHIFT_28
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then revenue else null end) / nullif(avg(case when d.date between dateadd(days, -119, current_date) and dateadd(days, -30, current_date) then revenue else null end), 0) as revenue_90_AVG_SHIFT_28
     
     , avg(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then inflows else null end) as inflows_28_avg
     , sum(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then inflows else null end) as inflows_28_SUM
     
     , sum(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then inflows else null end) as inflows_35_sum
     , avg(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then inflows else null end) as inflows_35_avg
     
     , avg(case when d.date between dateadd(days, -15, current_date) and dateadd(days, -2, current_date) then transactions else null end) as transactions_14_AVG
     
     , avg(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then transactions else null end) as transactions_28_AVG
     , sum(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then transactions else null end) as transactions_28_SUM
    
     , avg(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then transactions else null end) as transactions_35_AVG
     , sum(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then transactions else null end) as transactions_35_SUM
     
     , avg(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then revenue else null end) / nullif(avg(case when d.date between dateadd(days, -119, current_date) and dateadd(days, -92, current_date) then revenue else null end), 0) as revenue_28_AVG_SHIFT_90
     , sum(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then revenue else null end) / nullif(sum(case when d.date between dateadd(days, -119, current_date) and dateadd(days, -92, current_date) then revenue else null end), 0) as revenue_28_SUM_SHIFT_90

     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then revenue else null end) / nullif(sum(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then revenue else null end), 0) as revenue_90_SUM_SHIFT_90
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then revenue else null end) / nullif(avg(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then revenue else null end), 0) as revenue_90_AVG_SHIFT_90

     
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then profitability else null end) as profitability_90_AVG
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then profitability else null end) as profitability_90_SUM
     
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then inflows else null end) as inflows_90_avg
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then inflows else null end) as inflows_90_sum
     
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then outflows else null end) as outflows_90_avg
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then outflows else null end) as outflows_90_sum
     
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then transactions else null end) as transactions_90_avg
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then transactions else null end) as transactions_90_sum
     
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then inflows else null end) / nullif(sum(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then inflows else null end), 0) as inflows_90_SUM_SHIFT_90
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then inflows else null end) / nullif(avg(case when d.date between dateadd(days, -181, current_date) and dateadd(days, -92, current_date) then inflows else null end), 0) as inflows_90_AVG_SHIFT_90
     
     , sum(case when d.date between dateadd(days, -15, current_date) and dateadd(days, -2, current_date) then revenue else null end) as revenue_14_sum
     , sum(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then revenue else null end) as revenue_28_sum
     , sum(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then revenue else null end) as revenue_35_sum
     , sum(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then revenue else null end) as revenue_90_sum
     
     , avg(case when d.date between dateadd(days, -15, current_date) and dateadd(days, -2, current_date) then revenue else null end) as revenue_14_avg
     , avg(case when d.date between dateadd(days, -29, current_date) and dateadd(days, -2, current_date) then revenue else null end) as revenue_28_avg
     , avg(case when d.date between dateadd(days, -36, current_date) and dateadd(days, -2, current_date) then revenue else null end) as revenue_35_avg
     , avg(case when d.date between dateadd(days, -91, current_date) and dateadd(days, -2, current_date) then revenue else null end) as revenue_90_avg,
     NULL::NUMBER(10,6) AS prof_score, 
     'DBT'::VARCHAR(36) as RUN_ID,
       current_timestamp::TIMESTAMP_NTZ as warehouse_create_date,
       current_timestamp::TIMESTAMP_NTZ as warehouse_update_date

    from dates_frame d
   
   GROUP BY ID, date_TD, partner_id)
    
    select *
    
    from aggregated 
    
    where date_td = current_date