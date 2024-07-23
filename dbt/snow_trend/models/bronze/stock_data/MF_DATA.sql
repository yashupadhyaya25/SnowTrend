{{
    config(
        schema = 'BRONZE',
        materialized='table'
    )
}}

with all_stock as (
    select * from {{ ref('ALL_MF') }}
)
 SELECT
*,MD5(concat(TICKER,'_',DATE)) TICKER_KEY,
IF(OPEN <= 0,False,IF(HIGH <= 0,False,IF(LOW <= 0,False,IF(CLOSE <= 0,False,True)))) VALID_ROW 
FROM all_stock