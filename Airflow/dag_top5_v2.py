from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable

TOP_COUNTRIES = (
    "https://drive.google.com/uc?id=1zO8ekHWx9U7mrbx_0Hoxxu6od7uxJqWw&export=download"
)
CHAT_ID = -1002325375017
BOT_TOKEN = Variable.get("TELEGRAM_TOKEN")
URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def send_message(context):
    date = context["ds"]
    dag_id = context["dag"]
    message = f"Huge success: Dag {dag_id} completed on {date}"
    response = requests.get(URL, params={"chat_id": CHAT_ID, "text": message})
    if response.status_code == 200:
        print("Success delivery report in telegram")
    else:
        print(f"Error delivery report in telegram: {response.status_code}")


default_args = {
    "owner": "Alexeeva Anastasia",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 2, 8),
    "schedule_interval": "@daily",
    "catchup": False,
}


@dag(default_args=default_args)
def top_5_airflow_2():
    @task()
    def get_data():
        top_countries = pd.read_csv(TOP_COUNTRIES)
        return top_countries

    @task(retries=1)
    def get_stat_countries(top_data):
        top_data_top_5 = (
            top_data.groupby("Country", as_index=False)
            .agg({"Customer Id": "count"})
            .rename(columns={"Customer Id": "Count"})
            .sort_values("Count", ascending=False)
        )
        top_data_top_5 = top_data_top_5.head(5)
        return top_data_top_5

    @task(on_success_callback=send_message)
    def print_data(stat_data, ds):
        print(f"Top countries for date {ds}")
        print(stat_data)

    top_data = get_data()
    stat_data = get_stat_countries(top_data)
    print_data(stat_data)


start = top_5_airflow_2()
