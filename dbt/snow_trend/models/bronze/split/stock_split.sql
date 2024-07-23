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
TICKER,DATE,SPLITS 
FROM all_stock where SPLITS > 0.0