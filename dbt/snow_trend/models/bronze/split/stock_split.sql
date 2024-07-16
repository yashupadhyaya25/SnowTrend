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
TICKER,DATE,SPLITS 
FROM all_stock where SPLITS > 0.0