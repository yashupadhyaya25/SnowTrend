create_stock_table = """
CREATE OR REPLACE TABLE {{params.database}}.{{params.schema}}.{{params.table}}
(
DATE TIMESTAMP,
OPEN DECIMAL(18,2),
HIGH DECIMAL(18,2),
LOW DECIMAL(18,2),
CLOSE DECIMAL(18,2),
VOLUME DECIMAL(18,2),
DIVIDENDS DECIMAL(18,2),
SPLITS DECIMAL(18,2),
LOAD_DATE TIMESTAMP
);
"""

check_if_exists = """
select to_boolean(count(1)) as exist_flag from {{params.database}}.information_schema.tables where table_schema = '{{ params.schema }}' and TABLE_CATALOG = '{{params.catalog}}' and table_name = '{{params.table}}';
"""

check_max_date = """
SELECT MAX(DATE) DATE FROM {{params.database}}.{{params.schema}}.{{params.table}}
"""