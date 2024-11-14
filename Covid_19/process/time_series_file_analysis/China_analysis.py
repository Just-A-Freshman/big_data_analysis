"""
US analysis和global analysis基本差不太多, 唯一需要注意的是要提前去看一下Pyecharts里面美国地图的
各个州的名字, 保证数据能对应上。
"""
from pyecharts.charts import Line, Timeline
from pyecharts import options as opts
from pyecharts.globals import ThemeType
from datetime import datetime
from common import DataProcess, Vistualize
import pandas as pd
import numpy as np
import json
import os
import re



CONFIRMED_FILE = "time_series_covid19_confirmed_global.csv"
DEATHS_FILE = "time_series_covid19_deaths_global.csv"
RECOVERED_FILE = "time_series_covid19_recovered_global.csv"
POPULATION_FILE = "../UID_ISO_FIPS_LookUp_Table.csv"


class DataAnalysis(object):
    os.chdir("../../dataset/csse_covid_19_data/csse_covid_19_time_series")

    def __init__(self):
        # 小数据量手动清洗, 避免在代码层面清洗了。直接在文件中把台湾数据归入中国
        self.province_chinese_map = self.get_province_state_chinese_name()
        self.china_population: pd.Series = self.get_china_population()
        self.processor =  DataProcess(self.china_population)
        self.confirmed_df: pd.DataFrame = self.get_china_covid_data(CONFIRMED_FILE)
        self.deaths_df: pd.DataFrame = self.get_china_covid_data(DEATHS_FILE)
        self.current_confirmed_df: pd.DataFrame = self.get_current_confirmer_data()

    def get_china_covid_data(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        # 筛选出中国的数据并转化成中文
        df = df[df["Country/Region"] == "China"]
        df["Province/State"] = df["Province/State"].apply(lambda x: self.province_chinese_map.get(x, x))
        df = df.set_index("Province/State")
        df = df.reindex(index=self.china_population.index)
        df.dropna(axis=0, how="any", inplace=True)
        datetime_col = [field for field in df.columns if re.match(r"\d+/\d+/\d+", field)]
        return df[datetime_col]

    def get_province_state_chinese_name(self) -> set:
        with open("../../outer_resources/China_English_map.json", "r", encoding="utf-8")as f:
            return json.load(f)
    
    def get_china_population(self) -> pd.Series:
        # 从UID_ISO_FIPS_LookUp_Table.csv里读取中国各个省市区的人口数量
        # 手动在文件中将Taiwan数据归入中国，避免额外代码处理
        population_df = pd.read_csv(POPULATION_FILE)
        china_population_df = population_df.loc[
            population_df["Country_Region"].isin(["China"]),
            ["Province_State", "Population"]
        ].dropna(axis=0, how="any")
        china_population_df["Province_State"] = china_population_df["Province_State"].\
            apply(lambda x: self.province_chinese_map.get(x, x))
        china_population_series = china_population_df["Population"]
        china_population_series.index = china_population_df["Province_State"]
        return china_population_series
    
    def get_current_confirmer_data(self):
        recovered_df: pd.DataFrame = self.get_china_covid_data(RECOVERED_FILE)
        provinces, columns = self.confirmed_df.index, self.confirmed_df.columns

        confirmed_matrix = self.confirmed_df.to_numpy()
        recovered_matrix = recovered_df.to_numpy()
        recovered_matrix = DataProcess.data_replenish(recovered_matrix)

        result = confirmed_matrix - recovered_matrix
        current_confirmed_df = pd.DataFrame(result, columns=columns, index=provinces)

        return current_confirmed_df

    def draw_timeline_map(self):
        def create_data_pair(proportion_df, person_count_df, date: str):
            proportion = proportion_df[date]
            person_count = person_count_df[date]
            data_pair = [
                (self.province_chinese_map.get(k), (v1, v2)) 
                for k, v1, v2 in zip(states, proportion, person_count)
            ]
            data_pair.append(("南海诸岛", (0, 0)))
            return data_pair
        
        confirmed_proportion = self.processor.devide_by_population(self.confirmed_df)
        deaths_proportion = self.processor.devide_by_population(self.deaths_df)
        current_confirmed_proportion = self.processor.devide_by_population(self.current_confirmed_df)

        states = self.china_population.index
        datetime_col = self.confirmed_df.columns
        visualizor = Vistualize(render_max=0.0003)
        timeline = Timeline(init_opts=opts.InitOpts(width="1420px", height="750px", theme=ThemeType.DARK))

        for date in datetime_col:
            data_pair_dict = {
                "确诊": create_data_pair(confirmed_proportion, self.confirmed_df, date),
                "死亡": create_data_pair(deaths_proportion, self.deaths_df, date),
                "当前确诊": create_data_pair(current_confirmed_proportion, self.current_confirmed_df, date)
            }

            map_chart = visualizor.create_map(data_pair_dict, "china", "中国新冠状况")
            date_obj = datetime.strptime(date, "%m/%d/%y")
            timeline.add(map_chart, time_point=date_obj.strftime("%Y/%m/%d"))

        visualizor.customize_timeline(timeline)
        timeline.render(f"../../visualize_result/中国确诊情况.html")

    def draw_line_chart(self):
        def get_dx(df) -> list:
            return np.diff(df.to_numpy(), axis=1, prepend=0).sum(axis=0).tolist()
        xaxis_data: list = self.confirmed_df.columns.to_list()
        yaxis_data: list = [get_dx(df) for df in (self.confirmed_df, self.deaths_df, self.current_confirmed_df)]
        yaxis_name: list = ["新增确诊人数", "新增死亡人数", "新增确诊净增人数"]
        Vistualize.draw_line_chart(xaxis_data, yaxis_data, yaxis_name, "中国疫情趋势")


if __name__ == "__main__":
    data_analysis = DataAnalysis()
    # data_analysis.draw_timeline_map()
    # vistualizor = Vistualize()
    # vistualizor.draw_timeline_bar(data_analysis.confirmed_df, "中国确诊人数省份排名")
    data_analysis.draw_line_chart()
    