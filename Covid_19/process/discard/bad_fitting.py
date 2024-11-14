"""
①尝试使用了一次线性函数、幂函数、对数函数和指数函数进行拟合, 拟合效果均不佳; 
高次线性拟合尽管效果不错, 但是不能保证单调递增, 进而可能导致拟合结果为负数。
而康复数据明显为正数且单调递增, 这就会导致严重的数据错误。
②在没能较好拟合后, 我发现我忽略了一点即拟合前, 应该提前预览数据样貌。观察数据, 发现数据呈阶梯状。 
猜想是因为数据上报具有滞后性，在长时间没有上报的地方就简单采用了前面的数据进行填充。
③进一步猜测突变点是真正上报的数据, 尝试使用一次斜率大于0的函数对这部分突变数据进行拟合, 
但是拟合结果明显偏高。过高的康复数据导致了"累积确诊 - 累积康复"变成了负数, 也不合理。
④因此最后还是决定采用最质朴的方法, 即缺失部分直接采用前面的数据, 避免对康复数据过高的预期。
"""
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os


RECOVERED_FILE = "time_series_covid19_recovered_global.csv"


class DataAnalysis(object):
    os.chdir("../../../dataset/csse_covid_19_data/csse_covid_19_time_series")

    def __init__(self):
        self.good_fitting_count = 0
        self.recovered_data = self.get_china_covid_data(RECOVERED_FILE)
        self.provinces = self.recovered_data.index

    def get_china_covid_data(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        df = df[df["Country/Region"] == "China"].set_index("Province/State")
        df.dropna(axis=0, how="any", inplace=True)
        datetime_col = [field for field in df.columns if re.match(r"\d+/\d+/\d+", field)]
        return df[datetime_col]
    
    def data_replenish(self, matrix: np.ndarray) -> np.ndarray:
        flipped_matrix = np.fliplr(matrix)
        flipped_zero_indices = np.argmin(flipped_matrix == 0, axis=1)
        zero_indices = matrix.shape[1] - flipped_zero_indices
        for row, col, province in zip(matrix, zero_indices, self.provinces):

            print(f"省份: {province}, 数据量: {col}")
            X, Y = np.arange(col), row[:col]
            result = [self.line_fitting_model(X, Y, i) for i in range(4)]
            if any(result):
                self.draw_line_chart(province, X, Y)
                self.good_fitting_count += 1
        return matrix
    
    def line_fitting_model(self, X, Y, degree):
        coefficients = np.polyfit(X, Y, degree)
        model = np.poly1d(coefficients)
        residuals = Y - model(X)
        R = self.judge_if_good(Y, residuals)
        if R > 0.85:
            print(f"{degree}: {R}")
            return True
        
    @staticmethod
    def draw_line_chart(province, keys, values):
        plt.figure(figsize=(10, 5))  # 设置图形大小
        plt.plot(keys, values, marker='o')  # 绘制折线图，带有标记点
        plt.title(f'{province} current confimed condition')
        plt.xlabel("year")
        plt.ylabel("person confimed count")
        plt.grid(True)
        plt.show()

    def judge_if_good(self, Y, residuals):
        # 计算评估指标
        sse = np.sum(residuals**2)
        sst = np.sum((Y - np.mean(Y))**2)
        r_squared = 1 - sse/sst
        return r_squared

    @staticmethod
    def exponential_func(x, a, b):
        return a * np.exp(b * x)
    
    @staticmethod
    def logarithmic_func(x, a, b):
        return a + b * np.log(x)


if __name__ == "__main__":
    data_analysis = DataAnalysis()
    data_analysis.data_replenish(data_analysis.recovered_data.to_numpy())
    print(f"较好的拟合有: {data_analysis.good_fitting_count}个, 还差{34 - data_analysis.good_fitting_count}个需要拟合。")
