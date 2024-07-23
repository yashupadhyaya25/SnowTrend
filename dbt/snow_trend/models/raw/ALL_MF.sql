{{
    config(
        schema = 'RAW'
    )
}}

select *,'AXIS_BLUECHIP_FUND_DIRECT_PLAN_GROWTH' TICKER from {{ source('STOCK', 'AXIS_BLUECHIP_FUND_DIRECT_PLAN_GROWTH') }}