"""
US analysis和global analysis基本差不太多, 唯一需要注意的是要提前去看一下Pyecharts里面美国地图的
各个州的名字, 保证数据能对应上。
"""
from pyecharts.charts import Timeline
from pyecharts import options as opts
from pyecharts.globals import ThemeType
from common import DataProcess, Vistualize
from datetime import datetime
import pandas as pd
import json
import re
import os


# 美国没有的time_series的康复数据, 需要从csse_covid_19_daily_reports_us中手动聚合

CONFIRMED_FILE = "time_series_covid19_confirmed_US.csv"
DEATHS_FILE = "time_series_covid19_deaths_US.csv"
RECOVERED_FILE = "time_series_covid19_recovered_US.csv"


class DataAnalysis(object):
    # UID,iso2,iso3,code3,FIPS,Admin2,Province_State,Country_Region,Lat,Long_,Combined_Key
    os.chdir("../../dataset/csse_covid_19_data/csse_covid_19_time_series")

    def __init__(self):
        self.us_population: pd.Series = self.get_us_state_population()
        self.processor = DataProcess(self.us_population)
        self.confirmed_df: pd.DataFrame = self.get_us_covid_data(CONFIRMED_FILE)
        self.deaths_df: pd.DataFrame = self.get_us_covid_data(DEATHS_FILE)
        self.current_confirmed_df: pd.DataFrame = self.get_current_confirmer_data()

    def get_us_covid_data(self, path: str) -> pd.DataFrame:
        # data_df.groupby("Province_State", as_index=True)[datetime_col].sum()
        df = pd.read_csv(path)
        datetime_col = [field for field in df.columns if re.match(r"\d+/\d+/\d+", field)]
        df = df.groupby("Province_State", as_index=True)[datetime_col].sum()
        df = df.reindex(index=self.us_population.index)
        return df

    def get_us_state_name(self) -> set:
        with open("../../outer_resources/USA.json")as f:
            return set(json.load(f))
    
    def get_us_state_population(self) -> pd.Series:
        # 注意, deaths_df比confirmed_df多出Population列, 刚好可以拿着这列数据作为分母
        population_orig_df = pd.read_csv(DEATHS_FILE).set_index("Province_State")
        necessary_state_data: set = self.get_us_state_name()
        population_col: pd.Series = population_orig_df["Population"]
        state_population: pd.DataFrame = population_col.groupby(level=0).sum()
        state_population = state_population[state_population.index.isin(necessary_state_data)]
        return state_population
    
    def get_current_confirmer_data(self):
        states, columns = self.confirmed_df.index, self.confirmed_df.columns

        recovered_matrix: pd.DataFrame = self.get_us_covid_data(RECOVERED_FILE).to_numpy()
        confirmed_matrix = self.confirmed_df.to_numpy()

        result = confirmed_matrix - recovered_matrix
        current_confirmed_df = pd.DataFrame(result, columns=columns, index=states)

        return current_confirmed_df
    
    def javascript_code(self) -> str:
        js_code_str='''
            function(x) {
                let proportion = x.data.value[0] * 100; 
                if (x.seriesName === '死亡'){
                    return `${x.name}: ${Number((proportion / 70).toFixed(2))}%, ${x.data.value[1]}`; 
                }else{
                    return `${x.name}: ${Number(proportion.toFixed(2))}%, ${x.data.value[1]}`;
                }
            }
        '''
        return js_code_str

    def draw_timeline_map(self):
        def create_data_pair(proportion_df, person_count_df, date: str):
            proportion = proportion_df[date]
            person_count = person_count_df[date]
            data_pair = [(k, (round(v1, 4), v2)) for k, v1, v2 in zip(states, proportion, person_count)]
            return data_pair
        
        visualizor = Vistualize(None, 1, 0.5)
        visualizor.javascript_code = self.javascript_code()
        confirmed_proportion = self.processor.devide_by_population(self.confirmed_df)
        deaths_proportion = self.processor.devide_by_population(self.deaths_df * 70)
        current_confirmed_proportion = self.processor.devide_by_population(self.current_confirmed_df)
        states = confirmed_proportion.index
        datetime_col = self.confirmed_df.columns
        timeline = Timeline(init_opts=opts.InitOpts(width="1420px", height="750px", theme=ThemeType.DARK))

        for date in datetime_col:
            data_pair_dict = {
                "确诊": create_data_pair(confirmed_proportion, self.confirmed_df, date),
                "死亡": create_data_pair(deaths_proportion, self.deaths_df, date),
                "当前确诊": create_data_pair(current_confirmed_proportion, self.current_confirmed_df, date)
            }
            map_chart = visualizor.create_map(data_pair_dict, "美国", "美国新冠情况")
            date_obj = datetime.strptime(date, "%m/%d/%y")
            timeline.add(map_chart, time_point=date_obj.strftime("%Y/%m/%d"))

        visualizor.customize_timeline(timeline)
        timeline.render(f"../../visualize_result/美国新冠情况.html")


if __name__ == "__main__":
    data_analysis = DataAnalysis()
    data_analysis.draw_timeline_map()
    
