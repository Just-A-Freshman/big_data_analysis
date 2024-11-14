from pyecharts.charts import Timeline, Line
from pyecharts import options as opts
from pyecharts.globals import ThemeType
from common import DataProcess, Vistualize
from datetime import datetime
import numpy as np
import pandas as pd
import json
import os
import re


CONFIRMED_FILE = "time_series_covid19_confirmed_global.csv"
DEATHS_FILE = "time_series_covid19_deaths_global.csv"
RECOVERED_FILE = "time_series_covid19_recovered_global.csv"
POPULATION_FILE = "../UID_ISO_FIPS_LookUp_Table.csv"


class DataAnalysis(object):
    # Province/State,Country/Region,Lat,Long, 1/22/20-3/9/23
    os.chdir("../../dataset/csse_covid_19_data/csse_covid_19_time_series")

    def __init__(self):
        # 具备一种思想叫做提前统一, 尽快统一(格式)。
        self.population_series: pd.Series = self.get_global_country_population()
        self.processor = DataProcess(self.population_series)
        self.confirmed_df: pd.DataFrame = self.get_global_covid_data(CONFIRMED_FILE)
        self.deaths_df: pd.DataFrame = self.get_global_covid_data(DEATHS_FILE)
        self.current_confirmed_df: pd.DataFrame = self.get_current_confirmer_data()

    def get_global_covid_data(self, path: str) -> pd.DataFrame:
        # 在读取完数据后就要尽快groupby+reindex, 统一格式。
        # 统一格式: index是国家, columns是日期。国家顺序按人口的DataFrame来
        df = pd.read_csv(path)
        df["Country/Region"] = df["Country/Region"].apply(lambda x: self.unfind_country_map.get(x, x))
        datetime_col = [col for col in df.columns if re.match(r"\d+/\d+/\d+", col)]
        df = df.groupby("Country/Region", as_index=True)[datetime_col].sum()
        df = df.loc[df.index.isin(self.population_series.index)]
        df = df.reindex(self.population_series.index).fillna(0)
        return df
    
    def get_global_country_population(self):
        countries = self.pyecharts_countries
        lookup_table_df = pd.read_csv(POPULATION_FILE)
        lookup_table_df["Country_Region"] = lookup_table_df["Country_Region"].\
            apply(lambda x: self.unfind_country_map.get(x, x))
        lookup_table_df = lookup_table_df.set_index("Country_Region")
        cond = lookup_table_df["Province_State"].isna() & lookup_table_df.index.isin(countries)
        return lookup_table_df.loc[cond, "Population"].fillna(1)
    
    def get_current_confirmer_data(self):
        recovered_df = self.get_global_covid_data(RECOVERED_FILE)
        confirmed_matrix = self.confirmed_df.to_numpy()
        recovered_matrix = recovered_df.to_numpy()
        np.nan_to_num(recovered_matrix, copy=False)
        recovered_matrix = DataProcess.data_replenish(recovered_matrix)

        result = confirmed_matrix - recovered_matrix
        current_confirmed_df = pd.DataFrame(result, columns=recovered_df.columns)
        current_confirmed_df.fillna(0, inplace=True)
        current_confirmed_df.index = recovered_df.index

        return current_confirmed_df
    
    @property
    def unfind_country_map(self) -> dict:
        with open("../../outer_resources/unfind_country_map.json", "r", encoding="utf-8")as f:
            return json.load(f)
    
    @property
    def pyecharts_countries(self) -> set:
        with open("../../outer_resources/countries.json", "r", encoding="utf-8")as f:
            return set(json.load(f))

    def draw_timeline_map(self):
        def create_data_pair(proportion_df, person_count_df, date: str):
            proportion = proportion_df[date]
            person_count = person_count_df[date]
            data_pair = [(k, (v1, v2)) for k, v1, v2 in zip(countries, proportion, person_count)]
            data_pair += [(k, (0, 0)) for k in uninclude_countries]
            return data_pair
        
        confirmed_proportion = self.processor.devide_by_population(self.confirmed_df)
        deaths_proportion = self.processor.devide_by_population(self.deaths_df)
        current_confirmed_proportion = self.processor.devide_by_population(self.current_confirmed_df)

        visualizor = Vistualize(None, 1, 0.1)
        countries = self.population_series.index
        datetime_col = self.confirmed_df.columns
        uninclude_countries = set(self.pyecharts_countries) - set(countries)
        timeline = Timeline(init_opts=opts.InitOpts(width="1420px", height="750px", theme=ThemeType.DARK))

        for date in datetime_col:
            data_pair_dict = {
                "确诊": create_data_pair(confirmed_proportion, self.confirmed_df, date),
                "死亡": create_data_pair(deaths_proportion, self.deaths_df, date),
                "当前确诊": create_data_pair(current_confirmed_proportion, self.current_confirmed_df, date)
            }

            map_chart = visualizor.create_map(data_pair_dict, "world", "全球新冠发展动态")
            date_obj = datetime.strptime(date, "%m/%d/%y")
            timeline.add(map_chart, time_point=date_obj.strftime("%Y/%m/%d"))

        Vistualize.customize_timeline(timeline)
        timeline.render(f"../../visualize_result/全球疫情感染情况.html")

    def draw_line_chart(self):
        def get_dx(df) -> list:
            return np.diff(df.to_numpy(), axis=1, prepend=0).sum(axis=0).tolist()
        xaxis_data: list = self.confirmed_df.columns.to_list()
        yaxis_data: list = [get_dx(df) for df in (self.confirmed_df, self.deaths_df, self.current_confirmed_df)]
        yaxis_name: list = ["新增确诊人数", "新增死亡人数", "新增确诊净增人数"]
        Vistualize.draw_line_chart(xaxis_data, yaxis_data, yaxis_name, "全球疫情趋势")


if __name__ == "__main__":
    data_analysis = DataAnalysis()
    # Vistualizor = Vistualize()
    # Vistualizor.draw_timeline_bar(data_analysis.current_confirmed_df, "全球确诊人数排行")
    # data_analysis.draw_timeline_map()
    data_analysis.draw_line_chart()
