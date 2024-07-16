{{
    config(
        schema='DATA',
        database='BRONZE',
        materialized='table'
    )
}}

with all_stock as (
    select * from {{ ref('all_stock') }}
)
SELECT
TICKER,DATE,DIVIDENDS 
FROM all_stock where DIVIDENDS > 0.0