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
 SELECT MD5(concat(TICKER,'_',DATE)) TICKER_KEY,
*,
IFF(OPEN <= 0,False,IFF(HIGH <= 0,False,IFF(LOW <= 0,False,IFF(CLOSE <= 0,False,True)))) VALID_ROW 
FROM all_stock