{{
    config(
        schema = 'BRONZE',
        materialized='table'
    )
}}

with all_stock as (
    select * from {{ ref('ALL_STOCK') }}
)
SELECT
TICKER,DATE,DIVIDENDS 
FROM all_stock where DIVIDENDS > 0.0