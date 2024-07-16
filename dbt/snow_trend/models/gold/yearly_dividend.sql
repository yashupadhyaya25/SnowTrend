{{
    config(
        schema='DATA',
        database='GOLD',
        materialized='table'
    )
}}

SELECT TICKER,YEAR(DATE) YEAR,SUM(DIVIDENDS) TOTAL_DIVIDEND FROM {{ref('stock_dividend')}}
GROUP BY TICKER,YEAR(DATE)
