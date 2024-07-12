{{
    config(
        schema='DATA',
        database='RAW'
    )
}}

select 'PTC' TICKER,* from {{ source('RAW_STOCK', 'PTC') }}
UNION ALL
select 'TATACHEM' TICKER,* from {{ source('RAW_STOCK', 'TATACHEM') }}
UNION ALL
select 'IOC' TICKER,* from {{ source('RAW_STOCK', 'IOC') }}
UNION ALL
select 'ONGC' TICKER,* from {{ source('RAW_STOCK', 'ONGC') }}