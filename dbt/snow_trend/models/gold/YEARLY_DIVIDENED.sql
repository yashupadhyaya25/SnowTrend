{{
    config(
        schema='GOLD',
        materialized='table'
    )
}}

SELECT TICKER,YEAR(DATE) YEAR,SUM(DIVIDENDS) TOTAL_DIVIDEND FROM {{ref('STOCK_DIVIDENED')}}
GROUP BY TICKER,YEAR(DATE)
