from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_COUNTRIES = (
    "https://drive.google.com/uc?id=1zO8ekHWx9U7mrbx_0Hoxxu6od7uxJqWw&export=download"
)
TOP_COUNTRIES_FILE = "top-countries.csv"

# 1. Написание тасков для DAG


def get_data():
    top_countries = pd.read_csv(TOP_COUNTRIES)
    top_data = top_countries.to_csv(index=False)

    with open(TOP_COUNTRIES_FILE, "w") as f:
        f.write(top_data)


def get_stat():
    top_data_df = pd.read_csv(TOP_COUNTRIES)
    top_data_top_5 = (
        top_data_df.groupby("Country", as_index=False)
        .agg({"Customer Id": "count"})
        .rename(columns={"Customer Id": "Count"})
        .sort_values("Count", ascending=False)
    )
    top_data_top_5 = top_data_top_5.head(5)
    with open("top_data_top_5.csv", "w") as f:
        f.write(top_data_top_5.to_csv(index=False, header=False))


def print_data():
    with open("top_data_top_5.csv", "r") as f:
        all_data = f.read()

    print(f"Top countries for date {datetime.today()}")
    print(all_data)


# 2. Указание параметров для DAG
default_args = {
    "owner": "Alexeeva Anastasia",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 2, 8),
}
schedule_interval = "0 12 * * *"

dag = DAG(
    "top_5_country", default_args=default_args, schedule_interval=schedule_interval
)

# 3. Заполнение  DAG тасками

t1 = PythonOperator(task_id="get_data", python_callable=get_data, dag=dag)

t2 = PythonOperator(task_id="get_stat", python_callable=get_stat, dag=dag)

t3 = PythonOperator(task_id="print_data", python_callable=print_data, dag=dag)

t1 >> t2 >> t3

# t1.set_downstream(t2)
# t2.set_downstream(t3)
