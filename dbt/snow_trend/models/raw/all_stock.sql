{{
    config(
        schema = 'RAW'
    )
}}

select *,'PTC' TICKER from {{ source('STOCK', 'PTC') }}
UNION ALL
select *,'TATACHEM' TICKER from {{ source('STOCK', 'TATACHEM') }}
UNION ALL
select *,'IOC' TICKER from {{ source('STOCK', 'IOC') }}
UNION ALL
select *,'ONGC' TICKER from {{ source('STOCK', 'ONGC') }}
UNION ALL
select *,'UPL' TICKER from {{ source('STOCK', 'UPL') }}
UNION ALL
select *,'TATAPOWER' TICKER from {{ source('STOCK', 'TATAPOWER') }}