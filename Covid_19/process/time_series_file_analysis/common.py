from pyecharts.charts import Map, Bar, Timeline, Line
from pyecharts import options as opts
from pyecharts.globals import ThemeType
from pyecharts.commons.utils import JsCode
from typing import Sequence
from collections import Counter
from datetime import datetime
import numpy as np
import pandas as pd


class Vistualize(object):
    def __init__(self, center: tuple=(105, 35), zoom: int = 1.6, render_max: int = 0.3):
        self.center = center
        self.zoom = zoom
        self.render_max = render_max
        self.javascript_code: str = '''
            function(x){
                if(x.data.value[0] === x.data.value[1]){return `${x.name}: No data`}
                return `${x.name}: ${Number(x.data.value[0] * 100).toFixed(2)}%, ${x.data.value[1]}`
            }
        '''
        
    def draw_timeline_bar(self, data_df: pd.DataFrame, title: str):
        timeline = Timeline(init_opts=opts.InitOpts(width="1420px", height="750px", theme=ThemeType.DARK))

        for date in data_df.columns:
            data_dict = data_df[date].to_dict()
            bar = self.create_bar(data_dict, title)
            date_obj = datetime.strptime(date, "%m/%d/%y")
            timeline.add(bar, time_point=date_obj.strftime("%Y/%m/%d"))

        self.customize_timeline(timeline)
        timeline.add_schema(play_interval=1000)
        timeline.render(f"../../visualize_result/{title}.html")

    @staticmethod
    def draw_line_chart(
            xaxis_data: list, 
            yaxis_data: Sequence[Sequence[int|float]], 
            yaxis_name: Sequence[Sequence[str]], 
            color_list: Sequence[str] = ("#ABDEF2", "#3BD957", "#FFC029"),
            title: str="render"
        ):
        _line = Line()
        _line.add_xaxis(xaxis_data)
        for name, y_data, color in zip(yaxis_name, yaxis_data, color_list):
            _line.add_yaxis(
                name, y_data, label_opts=opts.LabelOpts(is_show=False), 
                is_symbol_show=False, areastyle_opts=opts.AreaStyleOpts(opacity=0.5, color=color)
            )
        _line.set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", range_start=0, range_end=len(xaxis_data)),  # 允许外部缩放
                opts.DataZoomOpts(type_="inside")  # 允许内部缩放
            ]
        )
        _line.render(f"../../visualize_result/{title}.html")

    def create_bar(self, data_dict: dict[str, int], title: str) -> Bar:
        counter = Counter(data_dict)
        top_20 = counter.most_common(20)[::-1]
        _bar = Bar()
        _bar.add_xaxis([i[0] for i in top_20])
        _bar.add_yaxis(title, [i[1] for i in top_20], label_opts=opts.LabelOpts(position="right"))
        _bar.reversal_axis()
        _bar.set_global_opts(title_opts=opts.TitleOpts(title))
        return _bar

    def create_map(self, data_pair_dict: dict, maptype: str, title: str) -> Map:
        # data_pair_dict形如: {"确诊": [('West Virginia', (0.358, 642760)),...], ...}
        _map = Map()
        for name, data_pair in data_pair_dict.items():
            _map.add(name, data_pair, zoom=self.zoom, center=self.center, maptype=maptype)
        _map.set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            visualmap_opts=opts.VisualMapOpts(max_=self.render_max, dimension=0),
            tooltip_opts=opts.TooltipOpts(formatter=JsCode(self.javascript_code))
        )
        _map.set_series_opts(
            label_opts=opts.LabelOpts(is_show=False),
            showLegendSymbol=False
        )
        return _map
    
    @staticmethod
    def customize_timeline(timeline: Timeline) -> None:
        timeline.add_schema(
            orient="vertical",
            is_inverse=True,
            pos_left="null",
            pos_right="5",
            pos_top="20",
            pos_bottom="20",
            width="90",
            label_opts=opts.LabelOpts(is_show=True, color="#fff"),
        )


class DataProcess(object):
    def __init__(self, population_df):
        self.population_df = population_df

    def devide_by_population(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # data_df默认已经和population_df的index对齐了
        population_matrix = self.population_df.to_numpy()
        data_matrix = data_df.to_numpy()
        population_matrix = population_matrix.reshape(data_matrix.shape[0], 1)
        result_matrix = data_matrix / population_matrix
        result_df = pd.DataFrame(result_matrix, index=data_df.index, columns=data_df.columns)
        result_df.fillna(0, inplace=True)
        return result_df

    @staticmethod
    def data_replenish(matrix: np.ndarray) -> np.ndarray:
        # 从右往左直至找到不为0的位置
        flipped_matrix = np.fliplr(matrix)
        flipped_zero_indices = np.argmin(flipped_matrix == 0, axis=1)
        zero_indices = matrix.shape[1] - flipped_zero_indices
        for onedarray, idx in zip(matrix, zero_indices):
            onedarray[idx:] = onedarray[idx - 1]

        return matrix
        

    