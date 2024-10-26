from collections import Counter
import swifter
import modin.pandas as pd
import json
import ray
import re
import os


class DataManager(object):
    ROOT_PATH = "../dataset"
    PATH = f"{ROOT_PATH}/job.csv"
    SAVE_PATH = f"{ROOT_PATH}/job_clean.csv"
    EXTERNAL_PATH = f"{ROOT_PATH}/additional_data.json"

    @classmethod
    def load_external_data(cls):
        with open(cls.EXTERNAL_PATH, "r", encoding="utf-8") as file:
            return json.load(file)

    @classmethod
    def load_raw_data(cls):
        return pd.read_csv(cls.PATH)

    @classmethod
    def save(cls, save_df: pd.DataFrame):
        from sqlalchemy import create_engine
        engine = create_engine("mysql+pymysql://root:Amyj198711@localhost/data_analysis")
        save_df.to_sql(name="amoy_job_tmp", con=engine, index=False, if_exists="replace")
        # 使用有签名的utf_8_sig保存，或者自己重新在编辑器里面保存
        # save_df.to_csv(cls.SAVE_PATH, index=False, encoding="utf_8_sig")


class FillMissingValue(object):
    def __init__(self, data_frame, _external_data):
        # 缺失值的补充，个人认为是最富有挑战性的
        # 异常值的数量远没有缺失值多，因此往往直接drop掉也无伤大雅，但缺失值就相当复杂了
        self.data_frame: pd.DataFrame = data_frame
        self.external_data = _external_data

    def fill_missing_value(self):
        self.data_frame["num"].fillna(3, inplace=True)
        self.data_frame["num"].replace({"若干": 6}, inplace=True)
        self.data_frame["lang"].fillna("不限", inplace=True)
        self.data_frame["age"].fillna("不限", inplace=True)
        self.data_frame["salary"].fillna("面议", inplace=True)
        self.data_frame["welfare"].fillna("面议", inplace=True)
        self.data_frame["industry"].replace({"其他行业": None}, inplace=True)
        self.data_frame["industry"] = self.data_frame.swifter.apply(self.fill_missing_industry, axis=1)
        self.data_frame["company_type"] = self.data_frame.apply(self.fill_missing_company_type, axis=1)

    def fill_missing_company_type(self, row):
        if pd.isnull(row["company_type"]):
            return self.external_data["query_company_type"][row["company"]]
        return row["company_type"]

    def fill_missing_industry(self, row: pd.Series):
        if pd.notnull(row["industry"]):
            return row["industry"]

        position_prefix = row["position"][:2]
        query_cond = f"position.str.startswith('{position_prefix}')"
        words: Counter = Counter()

        try:
            query_result: pd.DataFrame = self.data_frame.query(query_cond)["industry"].dropna()
        except SyntaxError:
            return "其他"

        for string in query_result:
            for word in re.split(r'\W', string):
                words[word] += 1

        words.pop("", None)
        words.pop("其他行业", None)
        most_common_keyword = words.most_common(3)
        industry: list = [keyword[0] for keyword in most_common_keyword]
        return "其他" if len(industry) == 0 else ",".join(industry)


class FormatField(object):
    def __init__(self, data_frame, _external_data):
        self.data_frame = data_frame
        self.external_data = _external_data

    def format_field(self):
        self.data_frame["sex"].replace({"无": "不限"}, inplace=True)
        self.data_frame["lang"].replace({"其他": "不限"}, inplace=True)
        self.data_frame["lang"] = self.data_frame["lang"].apply(self.__language_format)
        self.data_frame["age"] = self.data_frame["age"].apply(lambda x: x.replace("岁至", "-").replace("岁", ""))
        self.data_frame["education"] = self.data_frame["education"].apply(lambda x: x[:2])
        self.data_frame["work_experience"] = self.data_frame["work_experience"].apply(self.__work_experience_format)
        self.data_frame["industry"] = self.data_frame["industry"].apply(self.__industry_format)
        self.data_frame["company_type"] = self.data_frame["company_type"].apply(self.__company_type_format)

    @staticmethod
    def __work_experience_format(row):
        experience_match = r"(不限)|(.*?)工作经验以上|(应届生)"
        match_result = re.match(experience_match, row)
        if match_result is None:
            return "其他"
        for res in match_result.groups():
            if res is not None:
                return res

    def __industry_format(self, row):
        industry_map: dict = self.external_data["industry_map"]
        for industry, pattern in industry_map.items():
            if re.search(pattern, row):
                return industry

        return "其他"

    @staticmethod
    def __language_format(row):
        res = re.match(r"(.*?)语", row)
        return "不限" if res is None else res.group(0)

    @staticmethod
    def __company_type_format(row):
        if re.search(r"民营|私企", row):
            return "民营/私企"
        elif re.search(r"台资|港资", row):
            return "台资/港资"

        company_type_pattern = r"私营股份制|合资|外资|上市公司|国营企业|事业单位|外资代表处"
        search_result = re.search(company_type_pattern, row)
        if search_result:
            return search_result.group(0)
        else:
            return "其他"


class DataCleaning(object):
    def __init__(self, data_frame, _external_data):
        self.data_frame: pd.DataFrame = data_frame
        self.external_data = _external_data

    def drop_redundant_data(self):
        self.data_frame.drop_duplicates(inplace=True)
        self.data_frame.drop("phone", axis=1, inplace=True)
        self.data_frame.drop("HR", axis=1, inplace=True)
        self.data_frame.drop("workplace", axis=1, inplace=True)
        self.data_frame.drop("address", axis=1, inplace=True)
        self.data_frame.drop("worktime", axis=1, inplace=True)

    def fill_missing_value(self):
        filler = FillMissingValue(self.data_frame, external_data)
        filler.fill_missing_value()

    def divestiture_field(self):
        def fill_lose_shift_type(row):
            if pd.isnull(row["shift_type"]):
                if pd.isnull(row["work_hours"]) and pd.isnull(row["work_days"]):
                    return None
                return "正常白班"
            return row["shift_type"]

        work_hour_regex_exp = r"([0-9.]+)小时/天"
        work_day_regex_exp = r"([0-9.]+天/周)|大小周"
        shift_type_regex_exp = r"(不定时工作制|正常白班|正常晚班|2班倒|3班倒)"
        worktime_field = self.data_frame["worktime"].str
        self.data_frame["work_hours"] = worktime_field.extract(work_hour_regex_exp, expand=False)
        self.data_frame["work_days"] = worktime_field.extract(work_day_regex_exp, expand=False)
        self.data_frame["shift_type"] = worktime_field.extract(shift_type_regex_exp, expand=False)
        self.data_frame["shift_type"] = self.data_frame.apply(fill_lose_shift_type, axis=1)

    def deal_abnormal_value(self):
        clean_data.column_type_transform()
        self.data_frame["work_hours"].replace({40.0: 8.0})
        abnormal_salary_cond = self.data_frame["salary"].str.contains(r'^.-', regex=True, na=False)
        self.data_frame.drop(self.data_frame[abnormal_salary_cond].index, inplace=True)
        abnormal_work_hours = self.data_frame["work_hours"] > 16
        self.data_frame.drop(self.data_frame[abnormal_work_hours].index, inplace=True)
        # 这里的copy()是极为关键的一环，他确保了self.data_frame[abnormal_salary_cond].copy()得到的是副本而不是视图
        # SettingWithCopyWarning往往不是由对应行的代码引发而来的，要逐个函数检查!!
        # self.data_frame = self.data_frame[abnormal_salary_cond].copy()

    def format_field(self):
        formatter = FormatField(self.data_frame, self.external_data)
        formatter.format_field()

    def column_type_transform(self):
        self.data_frame["num"] = self.data_frame["num"].astype("int16")
        self.data_frame["work_hours"] = self.data_frame["work_hours"].astype("float16")


if __name__ == "__main__":
    # 配置好环境，启动Modin
    ray.init()
    swifter.register_modin()
    os.environ["MODIN_CPUS"] = "3"  # 限制 Modin 使用的 CPU 数量

    # 数据清洗
    data = DataManager.load_raw_data()
    external_data = DataManager.load_external_data()
    clean_data = DataCleaning(data, external_data)
    clean_data.divestiture_field()
    clean_data.fill_missing_value()
    clean_data.deal_abnormal_value()
    clean_data.drop_redundant_data()
    clean_data.format_field()
    DataManager.save(data)
