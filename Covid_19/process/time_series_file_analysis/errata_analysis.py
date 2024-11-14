"""
尽管errata.csv文件的纠错已经作用于time_series文件中了, 但是作为未来的数据分析者，
假设我们需要errata.csv来手动纠错, 这时我们就不得不去尝试解析errata.csv了。
这里就是一个自己挖掘的任务, 看看自己能不能利用errata.csv找出纠错过的位置在哪里
# ['Update Date', 'File', 'Location', 'Field Updated', 'Old', 'New', 'Comments']
"""
import pandas as pd
import os
import re


class AnalysisErrata(object):
    PATH = "Errata.csv"
    """
    要想完全解析Errata.csv, 就要分别解析File, Location, Field Updated三个字段
    1. File: 找到对应的DataFrame
    2. Location: 找到对应的行
    3. Field Updated: 找到对应的列
    """
    def __init__(self):
        prefix = "time_series_covid19"
        self.errata_file = pd.read_csv(AnalysisErrata.PATH)
        self.confirmed_global = pd.read_csv(f"{prefix}_confirmed_global.csv")
        self.confirmed_US = pd.read_csv(f"{prefix}_confirmed_US.csv")
        self.deaths_global = pd.read_csv(f"{prefix}_deaths_global.csv")
        self.deaths_US = pd.read_csv(f"{prefix}_deaths_US.csv")
        self.recovered_global = pd.read_csv(f"{prefix}_recovered_global.csv")
        self.dataframes = {
            "confirmed_global": self.confirmed_global,
            "confirmed_US": self.confirmed_US,
            "confirmed_us": self.confirmed_US,
            "deaths_global": self.deaths_global,
            "deaths_US": self.deaths_US,
            "deaths_us": self.deaths_US,
            "recovered_global": self.recovered_global,
        }
        
    def check_abandon_file(self):
        # file_not_find_times: 21
        # file_not_find: {'cases_global', 'recovered_gl', 'confirmed_gl', 'covid19_global', 'deaths_gl'}
        # 这种在文件夹中不存在的文件名指定是废弃文件，需要删除这些数据行
        file_not_find_times = 0
        file_not_find: set = set()
        for row in self.errata_file.itertuples(index=False):
            file = "_".join(row.File.split("_")[-2:])[:-4]
            try:
                df = self.dataframes[file]
            except KeyError:
                file_not_find_times += 1
                file_not_find.add(file)

    def check_unfound_location(self):
        pass

    def confirm_modify_loc(self):
        error_times: int = 0
        total_row: int = 0
        old_hit = new_hit = 0
        cannot_find_location = set()
        for row in self.errata_file.itertuples(index=False):
            total_row += 1
            file: str = "_".join(row.File.split("_")[-2:])[:-4]
            location: str = row.Location
            field_update: str = row._3
            if pd.isna(field_update) or file not in self.dataframes:
                continue
            
            df = self.dataframes[file]
            field_update = re.split("[-;]", field_update)[0]
            if re.search("us", file, re.I):
                loc = df.loc[df["Combined_Key"]==location, field_update]
            else:
                loc = df.loc[df["Country/Region"]==location, field_update]
            if loc.empty:
                # 查看一下查询不到location的字段
                error_times += 1
                cannot_find_location.add(f"{location}; {file}")
            else:
                tmp = [str(loc.iloc[i]) for i in range(loc.shape[0])]
                if row.New in tmp:
                    new_hit += 1
                if row.Old in tmp:
                    old_hit += 1

        print(f"错误次数: {error_times}, 占比: {error_times/total_row}")
        print(f"new_命中次数: {new_hit}, 占比: {new_hit/total_row}")
        print(f"old_命中次数: {old_hit}, 占比: {old_hit/total_row}")
        from date_unify import write_in_file
        write_in_file(cannot_find_location)
        # 错误次数: 562, 占比: 0.008006610439936175
        # new_命中次数: 60656, 占比: 0.8641440620013677
        # old_命中次数: 927, 占比: 0.013206633234556644



if __name__ == "__main__":
    os.chdir("../../dataset/csse_covid_19_data/csse_covid_19_time_series")
    analysis_errata = AnalysisErrata()
    analysis_errata.confirm_modify_loc()

