from collections import Counter
import pandas as pd
import json


LOAD_PATH = "../dataset"
SAVE_PATH = "../visual_result"


class CompanyAgeAcceptance(object):
    @staticmethod
    def quantity_statistics():
        age_range_path = f"{LOAD_PATH}/年龄.csv"
        age_df = pd.read_csv(age_range_path)
        age_dict = Counter()
        for age_range, frequency in zip(age_df["age"], age_df["age_count"]):
            lb, ub = age_range.split("-")
            for i in range(int(lb), int(ub) + 1):
                age_dict[i] += frequency

        keys = sorted(age_dict.keys())
        values = [age_dict[key] for key in keys]
        return keys, values

    @staticmethod
    def draw_age_acceptance_line_chart(keys, values):
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10, 5))  # 设置图形大小
        plt.plot(keys, values, marker='o')  # 绘制折线图，带有标记点
        plt.title('company age acceptance')
        plt.xlabel("age")
        plt.ylabel("quantity")
        plt.grid(True)
        plt.show()


class WelfareWordCloud(object):
    @staticmethod
    def get_word_frequency() -> Counter:
        df: pd.DataFrame = pd.read_csv(f"{LOAD_PATH}/job_clean.csv")
        welfare_series: pd.Series = df["welfare"]
        welfare_dict = Counter()
        for welfare in welfare_series:
            for i in welfare.split("、"):
                welfare_dict[i] += 1
        return welfare_dict

    @staticmethod
    def welfare_word_cloud(welfare_dict: dict):
        from pyecharts import options as opts
        from pyecharts.charts import WordCloud
        from pyecharts.globals import SymbolType

        word_cloud = WordCloud()
        data_pair = sorted([*welfare_dict.items()], key=lambda x: x[1])
        word_cloud.add("", data_pair, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
        word_cloud.set_global_opts(title_opts=opts.TitleOpts())
        word_cloud.render(f"{SAVE_PATH}/wordcloud.html")


class Train(object):
    @staticmethod
    def load_train_data():
        clean_data = pd.read_csv(f"{LOAD_PATH}/job_clean.csv")
        train_data = clean_data[['education', 'work_experience', 'salary']][clean_data['job_type'] == '全职']
        train_data['work_experience'] = train_data['work_experience'].apply(
            lambda x: x if x not in ['应届生', '不限', '其它工作经验'] else '0'
        )
        train_data['salary'] = train_data['salary'].astype('int')
        train_data = train_data[(train_data['salary'] > 1000)]
        train_data.to_csv('train.csv', index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    with open(f"{LOAD_PATH}/additional_data.json", "r", encoding="utf-8") as f:
        welfare_data = json.load(f)
    welfare_dict_data = welfare_data["welfare_cloud"]
    WelfareWordCloud.welfare_word_cloud(welfare_dict_data)

