from datetime import datetime, timedelta

import pandas as pd

from airflow.decorators import dag, task

STAT_YEAR = 2001

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
def get_games_statistics():
    @task()
    def get_data():
        data = pd.read_csv("/root/airflow/datasets/vgsales.csv")
        return data[data.Year == STAT_YEAR]

    @task()
    def get_most_sales_game(data):
        return data.loc[data.Global_Sales.idxmax()].Name

    @task()
    def get_top_europe_genre(data):
        genres_sales = data.groupby("Genre", as_index=False).agg({"EU_Sales": "median"})
        top_sales = genres_sales.EU_Sales.iloc[0]
        top_europe_genre = genres_sales.query("EU_Sales == @top_sales")
        return top_europe_genre

    @task()
    def get_top_na_platform(data):
        platform_sales = data.query("NA_Sales > 1").groupby("Platform").Name.count()
        top_sales = platform_sales.sort_values(ascending=False).iloc[0]
        top_platform = platform_sales.reset_index().query("Name == @top_sales")
        return top_platform

    @task()
    def get_top_jp_publisher(data):
        jp_sales = data.groupby("Publisher").JP_Sales.mean()
        top_jp_sales = jp_sales.sort_values(ascending=False).iloc[0]
        top_jp_publisher = jp_sales.reset_index().query("JP_Sales == @top_jp_sales")
        return top_jp_publisher

    @task()
    def get_count_eu_better_jp(data):
        return len(data.query("EU_Sales > JP_Sales"))

    @task()
    def print_logs(logs_dict):
        print(f"GAMES SALES STATISTICS IN {STAT_YEAR}")
        print("-------------------------------")
        print(f"Most sales game in {STAT_YEAR}:")
        print(f"{logs_dict['most_sales']}\n")
        print(f"Most sales genres in Europe in {STAT_YEAR}:")
        print(f"{logs_dict['top_europe_genres']}\n")
        print(f"Platforms with most games with >1M sales in NA in {STAT_YEAR}:")
        print(f"{logs_dict['top_na_platforms']}\n")
        print(f"Best sales publishers in Japan in {STAT_YEAR}:")
        print(f"{logs_dict['top_jp_publishers']}\n")
        print(f"Count games with better sales in Europe than Japan in {STAT_YEAR}:")
        print(logs_dict["count_eu_better_jp"])
        print("-------------------------------")

    data = get_data()
    logs_dict = {
        "most_sales": get_most_sales_game(data),
        "top_europe_genres": get_top_europe_genre(data),
        "top_na_platforms": get_top_na_platform(data),
        "top_jp_publishers": get_top_jp_publisher(data),
        "count_eu_better_jp": get_count_eu_better_jp(data),
    }
    print_logs(logs_dict)


get_games_statistics()
