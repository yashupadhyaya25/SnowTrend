create_stock_table = """
CREATE TABLE {{params.database}}.{{params.table}} (
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
SELECT IF(count(1)=1,True,False) as exist_flag FROM information_schema.tables where table_schema ='{{params.database}}' and table_name = '{{params.table}}';
"""

check_max_date = """
SELECT MAX(DATE) DATE FROM {{params.database}}.{{params.table}}
"""