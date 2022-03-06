

import json
import os
from functools import partial
import numpy as np
from time import sleep
from functools import partial


from airflow.models import Variable
from airflow.models import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group, task

import os
from pathlib import Path
import datetime

import yfinance as yf
import pandas as pd


stock_list = ["TLSA","ABNB","BABA", "BIDU", "NTES", "AAPL"]

now = datetime.datetime.now().strftime("%Y-%m-%d")
ori_time = "2000-1-1"
work_base = "/home/ubuntu/data.finance/"

DAG_NAME = "stock-update"



def verify_file_datetime(file_name):

    file_stem = Path(file_name).stem
    idx = file_stem.find("updated")
    try:
        y_m_d = file_stem[idx::].split("-",)[1:4]
        y_m_d = [int(string) for string in y_m_d]
        y_m_d = datetime.datetime(*y_m_d)
        return y_m_d
    except:
        raise FileNotFoundError("This filename is out of regular: STOCK_NAME-updated-YEAR-MONTH-DAY")

def find_stock_csv(path, stock_name):

    path = Path(path)
    csv_path = list(path.glob(f"{stock_name}-updated-*-*-*.csv"))
    if not csv_path:
        return None
    return csv_path[0].name

def build_stock_df(stock_name, start, end):
    df = pd.DataFrame(yf.download(stock_name, 
                    start=start,
                    end=end,
                    progress='True'))
    df.rename(columns={ 'Date':'DATE',
                    'Open':'OPEN',
                    'High':'HIGH',
                    'Low':'LOW',
                    'Close':'CLOSE',
                    'Adj Close':'ADJCLOSE',
                    'Volume':'VOLUME'}, inplace=True)

    df.insert(0, 'TICKER', stock_name)

    return df


def update_stock_df(stock_name, stock_csv, end):
    
    last_date = verify_file_datetime(stock_csv)
    last_date = last_date + datetime.timedelta(days=1)
    last_date = last_date.strftime("%Y-%m-%d")
    df = build_stock_df(stock_name, start=last_date, end=end)

    return df


def check_stock_file(stock_list):

    founded_csv = []
    for stock in stock_list:
        founded_csv.append(find_stock_csv(work_base, stock))

    return founded_csv

def update_csv(founded_csv):

    os.chdir(work_base)

    for stock, stock_csv in zip(stock_list, founded_csv):

        if stock_csv:
            print(f"csv file has been found: {stock_csv}, begin to add new data..")
            new_stock_csv = f"{stock}-updated-{now}.csv"
            add_df = update_stock_df(stock, stock_csv, end=now)
            print("addeing new stock data: ",add_df)
            add_df.to_csv(stock_csv, mode="a",header=None)
            Path(stock_csv).rename(new_stock_csv)

            print(f"{stock} stock .csv has been updated to {now}")

        else:
            stock_csv = f"{stock}-updated-{now}.csv"
            print(f"no .csv file found, begin to establish {stock_csv}.")
            stock_data = build_stock_df(stock, start=ori_time, end=now)
            stock_data.to_csv(stock_csv, header=True)


default_parameters={}

with DAG(dag_id=DAG_NAME,
    default_args=default_parameters,
    schedule_interval=None, 
    start_date=days_ago(2),tags=["stock","yfinance"],) as dag:


    check_csv = task(check_stock_file, task_id="check_csv_exsits")(stock_list)
    update_stock = task(update_csv, task_id = "update_stock",)(check_csv)


    check_csv >> update_stock


    


