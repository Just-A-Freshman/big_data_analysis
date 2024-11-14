"""
聚合美国的康复数据, 基于多线程。为方便, 此处不再采用面向对象的写法，只使用面向过程的写法。
只保留省份+康复数据, 其他删除
观察发现time_series_covid19_confirmed_US.csv中数据实际更详细, 因为包含了各个地区的详细数据
而单日报告只是整个州的数据
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import re


# 思路: 
# ①先找出最完整的States对应的文件, 
# ②然后获取这个文件的States,
# ③然后依据这个States逐个匹配
DATE_FORMAT = "%m/%d/%y"
LOSE_START_DATE = datetime.strptime("1/22/20", DATE_FORMAT)
LOSE_END_DATA = datetime.strptime("4/11/20", DATE_FORMAT)
BASE_PATH = "../../dataset/csse_covid_19_data"
REPORTS_US_DIR = os.path.join(BASE_PATH, "csse_covid_19_daily_reports_us")
SAVE_PATH = os.path.join(BASE_PATH, "csse_covid_19_time_series/time_series_covid19_recovered_US.csv")


def get_max_state_file():
    def task(path):
        states = pd.read_csv(path)["Province_State"].to_list()
        return len(states), path
    
    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = [
            pool.submit(task, i) for i in os.scandir(REPORTS_US_DIR) 
            if i.name.endswith(".csv")
        ]
        max_length = 0
        for future in futures:
            length, path = future.result()
            if length > max_length:
                max_length = length
                max_path = path
        return max_path


def get_states():
    df = pd.read_csv(os.path.join(REPORTS_US_DIR, "04-12-2020.csv"))
    df = df.set_index("Province_State")
    return df.index

def single_date_formatter(date_str) -> str:
    # 直接从date_unify那边拿过来的函数, 将04-12-2020 -> 4/12/20
    date_str = re.sub("\D", "/", date_str)
    match_res = re.match(r"0?(\d+/)0?(\d+/)(?:20)?(\d{2})$", date_str)
    return "".join(match_res.groups()) if match_res else ""

def get_dates_delta(start, end):
    list = []
    list.append(start.strftime(DATE_FORMAT))
    while start < end:
        start += timedelta(days=1)
        list.append(start.strftime(DATE_FORMAT))
    return list


def aggregation_data():
    def task(path, index) -> np.ndarray:
        df = pd.read_csv(path).set_index("Province_State")
        recover_data = df["Recovered"]
        recover_data = recover_data.reindex(index=index)
        return recover_data.to_numpy()

    futures = list()
    index = get_states()
    datetime_col = list()
    with ThreadPoolExecutor(max_workers=15) as pool:
        for i in os.scandir(REPORTS_US_DIR):
            if i.name.endswith(".csv"):
                datetime_col.append(i.name[:-4])
                futures.append(pool.submit(task, i.path, index))
    datetime_col = get_dates_delta(LOSE_START_DATE, LOSE_END_DATA) + datetime_col
    datetime_col = [single_date_formatter(date) for date in datetime_col]
    matrix = np.array([future.result() for future in futures]).T
    matrix = np.where(np.isnan(matrix), 0, matrix)
    accumulate = np.maximum.accumulate(matrix, axis=1)
    matrix = np.where(matrix == 0, accumulate, matrix)
    pad_width = ((0, 0), ((LOSE_END_DATA - LOSE_START_DATE).days + 1, 0))
    matrix = np.pad(matrix, pad_width, mode='constant', constant_values=0)
    agg_data = pd.DataFrame(matrix)
    agg_data.columns = datetime_col
    datetime_col = sorted(datetime_col, key=lambda x: datetime.strptime(x, "%m/%d/%y"))
    agg_data = agg_data[datetime_col]
    agg_data.index = index
    agg_data.to_csv(SAVE_PATH)


if __name__ == "__main__":
    aggregation_data()

    