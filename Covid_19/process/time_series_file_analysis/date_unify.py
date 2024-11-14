"""
通过检查发现, Errata.csv里面的Field Updated字段里的日期格式十分混乱,包括time_series_xxx
等一系列文件的日期格式也不统一。此时需要分析他们存在的不同格式，并进行严格统一，以实现
Errata.csv与time_series_xxx等文件的对应。
"""
import pandas as pd
import re
import os


def write_in_file(tmp_data):
    # 获取绝对路径
    tmp_file_path = r"tmp.txt"
    with open(tmp_file_path, "w", encoding="utf-8")as f:
        for i in tmp_data:
            if not pd.isna(i):
                f.write(i)
                f.write("\n")


class CheckDateFormat(object):
    ## step1: 确定Errata.csv里面有哪些格式
    @staticmethod
    def get_errata_file_date_format():
        errata_file = pd.read_csv("Errata.csv")
        format_set = set()
        for row in errata_file.itertuples(index=False):
            format_set.add(row._3)
        write_in_file(format_set)
        # 排查格式的思路: 写入文件后，看到一种格式，就尝试用正则表达式匹配，匹配上后就替换删去，继续看其他格式
        # 在检查格式时，还发现一个" 11/23/2020"没有被^0\d+/\d+/\d{4}$匹配上，这是一个个例, 因为其前面有一个空格，需要人手动删除
        # 目前Errata.csv中存在的格式有: 
        # ①05/05/2021或6/4/2022 -- 0?(\d+/)0?(\d+/)20(\d{2})
        # ②All columns -- ^all.*
        # ③7/20-7/23/2020 -- ^.*?-.*    
        # ④03/13/20;04/22/20 -- .*?;.*
        # ⑤极少数特殊的格式4个，尝试手动处理了，代码额外编写逻辑费劲:
        # 09/15/2020, 09/16/2020, 09/17/2020
        # 10/20/2020, 10/17/2020, 10/9/2020 10/8/2020
        # before 4/19
        # 10/23/24/28 and 11/1   10/23/28 and 11/1

    ## step2: 确定time_series_xxx里面有哪些格式
    @staticmethod
    def get_time_series_file_date_format():
        format_set = set()
        for file in os.scandir("."):
            if file.is_file() and file.name.startswith("time_series"):
                df = pd.read_csv(file.name)
                for i in df.columns:
                    if not re.search("\d+/", i):
                        continue
                    format_set.add(i)
        write_in_file(format_set)
        # 比较欣慰的是series_file里面的date格式还是比较统一的，有且仅有两种
        # 因此思路就是，先统一series_file里面的格式, 然后让errata的格式去和series_file进行统一
        # ①3/14/21 -- ^[1-9]\d?/\d+/\d{2}$     1143条
        # ②2006/3/20 -- ^20\d{2}/\d+/\d{2}$    453条



class UnifyDateFormat(object):
    # step3: 统一series_file里面的格式
    @staticmethod
    def series_file_date_format_unify():
        # 统一格式很简单，就将格式如2006/3/20 -> 6/3/20, 很明显这里2006不是指2006年,
        # 根据日期的连续性就知道其含义是6/3/20
        for file in os.scandir("."):
            if file.is_file() and file.name.startswith("time_series"):
                df = pd.read_csv(file.name)
                new_columns = []
                for i in df.columns:
                    match_result = re.match(r"^20{1,2}(\d+/\d+/\d{2})$", i)
                    if match_result:
                        i = match_result.group(1)

                    new_columns.append(i)
                df.columns = new_columns
                df.to_csv(file.name, index=False)

    
    # step4: 统一errata_file里面的格式
    def errata_file_date_format_unify(self):
        errata_file = pd.read_csv("Errata.csv")
        for row_num, row in enumerate(errata_file.itertuples(index=False)):
            if pd.isna(row._3):
                print(row.Comments)
                continue

            date = self.__single_date_formatter(row._3)

            if date == "":
                date = self.__date_range_formatter(row._3)
            
            if date == "":
                if ";" in row._3:
                    date_list = row._3.split(";")
                    for i, old_date in enumerate(date_list):
                        format_date = self.__single_date_formatter(old_date)
                        if format_date == "":
                            format_date = self.__date_range_formatter(old_date)
                        date_list[i] = format_date
                    date = ";".join(date_list)
            
                elif re.match("^all.*", row._3, re.I):
                    date = "1/23/20-3/9/23"
            errata_file.iat[row_num, 3] = date

        errata_file.to_csv("Errata_copy.csv", index=False)

    @staticmethod
    def __single_date_formatter(date_str) -> str:
        match_res = re.match(r"0?(\d+/)0?(\d+/)(?:20)?(\d{2})$", date_str)
        return "".join(match_res.groups()) if match_res else ""
    
    @staticmethod
    def __date_range_formatter(date_str) -> str:
        pattern = r"^0?(\d+/)0?(\d+)/?(20)?(\d{2})?\s*-\s*0?(\d+/)0?(\d+/)(?:20)?(\d{2})$"
        match_res = re.match(pattern, date_str)
        if not match_res:
            return ""
        g1, g2, _, g4, g5, g6, g7 = match_res.groups()
        if g4 is None:
            return f"{g1}{g2}/{g7}-{g5}{g6}{g7}"
        else:
            return f"{g1}{g2}/{g4}-{g5}{g6}{g7}"
    
if __name__ == "__main__":
    os.chdir("../../dataset/csse_covid_19_data/csse_covid_19_time_series")
    # unify_formatter = UnifyDateFormat()
    # unify_formatter.errata_file_date_format_unify()
    checker = CheckDateFormat()
    checker.get_errata_file_date_format()
    
