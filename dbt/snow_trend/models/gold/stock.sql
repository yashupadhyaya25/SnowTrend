{{
    config(
        schema='GOLD',
        materialized='table'
    )
}}
SELECT TICKER,TICKER_KEY,DATE,LOW,HIGH,CLOSE,OPEN,LOAD_DATE from {{ref('STOCK_DATA')}}