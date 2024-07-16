from datetime import datetime
import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow import DAG

with DAG(
    "dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag :

    profile_config = ProfileConfig(profile_name="snow_trend",
                                target_name="dev",
                                profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id="snowflake_db",
                                profile_args={
                                "database": "RAW",
                                "schema": "STOCK"
                                })
                                                        )


    dbt_snowflake_dag = DbtDag(project_config=ProjectConfig("/root/snow_trend",),
                        operator_args={"install_deps": True},
                        profile_config=profile_config,
                        execution_config=ExecutionConfig(dbt_executable_path=f"/usr/bin/dbt",),
                        schedule_interval="@daily",
                        start_date=datetime(2023, 9, 10),
                        catchup=False,
                        dag_id="snow_trend",)