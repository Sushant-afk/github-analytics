from airflow.decorators import dag
from airflow.operators.python import get_current_context
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SPARK_CONTAINER = "spark_jupyter_app"  # exact name from `docker ps`

@dag(
    schedule="@hourly",
    start_date=datetime(2026, 7, 1),
    catchup=False,
)
def github_analytics():

    def fetch_time_str(context):
        return (context["logical_date"] - timedelta(hours=6)).isoformat()

    
    # BashOperator will create a task for my dag
    # SPARK SUBMIT WILL HELP ME RUN MY SPARK CODE IN THE PYTHON FILE AS WE CAN'T DO python xyz.py for spark code
    bronze_task = BashOperator(
        task_id="bronze",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit "
            "/app/src/bronze/ingestor.py "
            "{{ (data_interval_end - macros.timedelta(hours=6)).isoformat() }}"
        ),
    )

    silver_task = BashOperator(
        task_id="silver",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit "
            "--packages org.apache.hadoop:hadoop-aws:3.4.2 "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "/app/src/silver/silver.py "
            "{{ (data_interval_end - macros.timedelta(hours=6)).isoformat() }}"
        ),
    )

    gold_task = BashOperator(
        task_id="gold",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit "
            "--packages org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.3 "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "/app/src/gold/staging.py "
            "{{ (data_interval_end - macros.timedelta(hours=6)).isoformat() }}"
        ),
    )

    bronze_task >> silver_task >> gold_task


dag = github_analytics()