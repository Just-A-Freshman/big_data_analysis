"""
该文件展示了最初get_current_confirmed_data的写法;
没有使用reindex的方法, 而是使用了for循环。
让recover_matrix与confirmed_matrix的Country/Region列逐个匹配。
reindex作为pandas的内置方法, 避免了python中显式的for循环, 显著提高了性能以及可读性。 
"""
import pandas as pd
import numpy as np


CONFIRMED_FILE = "time_series_covid19_confirmed_global.csv"
RECOVERED_FILE = "time_series_covid19_recovered_global.csv"


class DataAnalysis(object):
    def __init__(self):
        self.confirmed_df: pd.DataFrame = pd.read_csv(CONFIRMED_FILE)
        self.recovered_df: pd.DataFrame = pd.read_csv(RECOVERED_FILE)

    def get_current_confirmer_data(self):
        group_by_param = {"by": "Country/Region", "as_index": False}
        datetime_col = self.confirmed_df.columns[4:]
        sum_confirmed = self.confirmed_df.groupby(**group_by_param)[datetime_col].sum()
        sum_recovered = self.recovered_df.groupby(**group_by_param)[datetime_col].sum()
        
        confirmed_matrix = sum_confirmed[datetime_col].to_numpy()
        recovered_matrix = np.zeros_like(confirmed_matrix)

        # 使用循环让recover_matrix与confirmed_matrix的Country/Region列逐个匹配。
        for index, country in enumerate(sum_confirmed["Country/Region"]):
            match_row: pd.DataFrame = sum_recovered[sum_recovered["Country/Region"]==country]
            recovered_array: np.ndarray = match_row.iloc[:,1:].to_numpy()
            if recovered_array.size != 0:
                recovered_matrix[index] = recovered_array
            
        result = confirmed_matrix - recovered_matrix
        current_confirmed_df = pd.DataFrame(result, columns=datetime_col)
        current_confirmed_df["Country/Region"] = sum_confirmed["Country/Region"]
        return current_confirmed_df
    