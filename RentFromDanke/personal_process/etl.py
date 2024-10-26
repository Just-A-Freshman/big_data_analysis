import pandas as pd
import numpy as np
import os

DATASET = r"../dataset/"


class DataProcessing(object):
    def __init__(self):
        self.data_frame: pd.DataFrame | None = None

    @staticmethod
    def check_columns():
        # 检查数据的行列基本情况。如果数据结构上一致就设法合并，便于分析。
        for path in os.scandir(os.path.join(DATASET, "row_csv")):
            if path.name.endswith(".zip"):
                continue
            tmp_df = pd.read_csv(path.path)
            print([(i, str(tmp_df[i].dtype)) for i in tmp_df.columns])
            print("-" * 60)

    def data_concat(self):
        concat_list = list()
        for path in os.scandir(os.path.join(DATASET, "row_csv")):
            if path.name.endswith(".csv"):
                concat_list.append(pd.read_csv(path.path))
        self.data_frame = pd.concat(concat_list)

        # 洗一次写一次，解耦合并且能更灵活的手动处理一些极少数的一些异常数据, 无需大量代码
        self.data_frame.dropna(axis=0, how="all", inplace=True)
        self.data_frame.drop_duplicates(inplace=True)
        self.data_frame.to_csv(os.path.join(DATASET, "clean_1.csv"), index=False, encoding="utf_8_sig")

    def csv_process(self):
        # 拆 -> 丢 -> 填 -> 格式化, 这才是符合数据处理的顺序
        self.data_frame = pd.read_csv(os.path.join(DATASET, "clean_1.csv"))
        
        # 字段拆分
        self.data_frame[['室', '卫']] = self.data_frame['户型'].str.extract(r'(\d+)室(\d+)卫')
        self.data_frame[['所在楼层', '总楼层']] = self.data_frame["楼层"].str.extract(r'(\d+)/(\d+)层')
        self.data_frame[['地铁站名', "地铁距离"]] = self.data_frame["地铁"].str.extract(r"距(.*?站)(\d+)米")

        # 舍弃冗余字段
        self.data_frame.drop(["编号", "户型", "楼层", "地铁"], axis=1, inplace=True)

        # 填充空值
        self.data_frame["地铁站名"] = self.data_frame["地铁站名"].fillna("无")
        self.data_frame["地铁距离"] = self.data_frame["地铁距离"].fillna(65535)
        
        # 字段顺序调整
        self.data_frame = self.data_frame[
            ["价格", "面积", "室", "卫", "所在楼层", "总楼层", "位置1", "位置2", "小区", "地铁站名", "地铁距离"]
        ]

        self.data_frame.to_csv(os.path.join(DATASET, "clean_2.csv"), index=False, encoding="utf_8_sig")
    
    def write_to_sql(self):
        self.data_frame = pd.read_csv(os.path.join(DATASET, "clean_2.csv"))

        # 字段类型转换
        self.data_frame["价格"] = self.data_frame["价格"].astype(np.uint16)
        self.data_frame["面积"] = self.data_frame["面积"].astype(np.uint8)
        self.data_frame["室"] = self.data_frame["室"].astype(np.uint8)
        self.data_frame["卫"] = self.data_frame["卫"].astype(np.uint8)
        self.data_frame["所在楼层"] = self.data_frame["所在楼层"].astype(np.uint8)
        self.data_frame["总楼层"] = self.data_frame["总楼层"].astype(np.uint8)
        self.data_frame["地铁距离"] = self.data_frame["地铁距离"].astype(np.uint16)

        # 写入数据库
        from sqlalchemy import create_engine
        engine = create_engine("mysql+pymysql://root:Amyj198711@localhost/data_analysis")
        self.data_frame.to_sql(name="rent", con=engine, index=False, if_exists="replace")

    """def price_process(self):
        # 下述代码废弃, 因为异常值很少并且很有特点, 手动处理比代码考虑各种情况要快得多
        prices = pd.to_numeric(self.data_frame["价格"], errors='coerce')
        valid_prices = prices.dropna()
        avg_val = valid_prices.mean()
        # 替换异常值
        self.data_frame["价格"] = self.data_frame["价格"].apply(
            lambda x: x if str(x).isdigit() else avg_val
        )
        self.data_frame["价格"] = self.data_frame["价格"].astype(np.uint16)
        # 在数值上，没有发现价格异常值, min=930, max=11740, 可以使用无符号整数uint16: 最大存到65535"""
    

if __name__ == "__main__":
    processor = DataProcessing()
    processor.write_to_sql()

