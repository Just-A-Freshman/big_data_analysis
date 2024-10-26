#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time : 2021/1/21 21:06
# @Author : way
# @Site : 
# @Describe: 数据处理
# etl - extract-transform-load的缩写

import re
import pandas as pd


class DataCleaning(object):
    def __init__(self, data_frame):
        self.data_frame: pd.DataFrame = data_frame

    def basic_cleaning(self):
        """
        预览重复行的数据，确定这些重复数据可以舍去
        duplicates = self.data_frame.duplicated(keep=False)
        duplicated_rows = self.data_frame[duplicates]
        print("行号：", duplicated_rows.index)
        """
        self.data_frame.drop_duplicates(inplace=True)
        self.data_frame.reset_index(drop=True, inplace=True)

        # 招聘人数处理：缺失值填 1 ，一般是一人; 若干人当成 3人
        self.data_frame['num'].fillna(1, inplace=True)
        self.data_frame['num'].replace('若干', 3, inplace=True)

        # 年龄要求：缺失值填 无限；格式化
        self.data_frame['age'].fillna('不限', inplace=True)
        self.data_frame['age'] = data['age'].apply(lambda x: x.replace('岁至', '-').replace('岁', ''))

        # 语言要求: 忽视精通程度，格式化
        self.data_frame['lang'].fillna('不限', inplace=True)
        self.data_frame['lang'] = data['lang'].apply(lambda x: x.split('水平')[0])
        self.data_frame['lang'].replace('其他', '不限', inplace=True)

        # 月薪: 格式化。根据一般经验取低值，比如 5000-6000, 取 5000
        self.data_frame['salary'] = data['salary'].apply(lambda x: x.replace('参考月薪： ', '') if '参考月薪： ' in str(x) else x)
        self.data_frame['salary'] = data['salary'].apply(lambda x: x.split('-', 1)[0] if '-' in str(x) else x)
        self.data_frame['salary'].fillna('0', inplace=True)

        # 其它岗位说明：缺失值填无
        self.data_frame.fillna('其他', inplace=True)

    def format(self):
        self.data_frame['work_experience'] = data['work_experience'].apply(self.__work_experience_clean)

        # 性别格式化
        self.data_frame['sex'].replace('无', '不限', inplace=True)

        # 工作类型格式
        self.data_frame['job_type'].replace('毕业生见习', '实习', inplace=True)

        # 学历格式化
        self.data_frame['education'].unique()
        self.data_frame['education'] = self.data_frame['education'].apply(lambda x: x[:2])

        self.data_frame['company_type'] = self.data_frame['company_type'].apply(self.__company_type_clean)

        self.data_frame['industry'] = self.data_frame['industry'].apply(self.__industry_clean)

        # 工作时间格式化
        self.data_frame['worktime_day'] = (self.data_frame['worktime'].
                                           apply(lambda x: x.split('小时')[0] if '小时' in x else 0))
        self.data_frame['worktime_week'] = (self.data_frame['worktime'].
                                            apply(lambda x: re.findall(r'\S*周', x)[0] if '周' in x else 0))

        # 从工作要求中正则解析出：技能要求
        self.data_frame['skill'] = self.data_frame['require'].apply(lambda x: '、'.join(re.findall('[a-zA-Z]+', x)))

    def save(self, check=False):
        # 查看保存的数据
        if check:
            print(self.data_frame.info)
        else:
            from sqlalchemy import create_engine
            engine = create_engine("mysql+pymysql://root:Amyj198711@localhost/data_analysis")
            self.data_frame.to_sql(name="std_amoy_job", con=engine, index=False, if_exists="replace")

    @staticmethod
    # 工作年限格式化
    def __work_experience_clean(x):
        if x in ['应届生', '不限']:
            return x
        elif re.findall(r'\d+年', x):
            return re.findall(r'(\d+)年', x)[0]
        elif '年' in x:
            x = re.findall(r'\S{1,2}年', x)[0]
            x = re.sub(r'[厂验年]，', '', x)
            digit_map = {
                '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
                '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15, '十六': 16, '两': 2
            }
            return digit_map.get(x, x)
        return '其它工作经验'

    @staticmethod
    def __company_type_clean(x):
        if len(x) > 100 or '其他' in x:
            return '其他'
        elif re.findall('私营|民营', x):
            return '民营/私营'
        elif re.findall('外资|外企代表处', x):
            return '外资'
        elif re.findall('合资', x):
            return '合资'
        return x

    # 行业 格式化。多个行业，取第一个并简单归类
    @staticmethod
    def __industry_clean(x):
        if len(x) > 100 or '其他' in x:
            return '其他'
        industry_map = {
            'IT互联网': '互联网|计算机|网络游戏', '房地产': '房地产', '电子技术': '电子技术', '建筑': '建筑|装潢',
            '教育培训': '教育|培训', '批发零售': '批发|零售', '金融': '金融|银行|保险', '住宿餐饮': '餐饮|酒店|食品',
            '农林牧渔': '农|林|牧|渔', '影视文娱': '影视|媒体|艺术|广告|公关|办公|娱乐', '医疗保健': '医疗|美容|制药',
            '物流运输': '物流|运输', '电信通信': '电信|通信', '生活服务': '人力|中介'
        }
        for industry, keyword in industry_map.items():
            if re.findall(keyword, x):
                return industry
        return x.split('、')[0].replace('/', '')


class Train(object):
    def __init__(self, data_frame):
        self.data_frame = data_frame

    def train(self):
        # 取学历、年龄段、薪资作预测，保存为 train.csv
        train_data = self.data_frame[['education', 'work_experience', 'salary']][self.data_frame['job_type'] == '全职']
        train_data['work_experience'] = train_data['work_experience'].apply(
            lambda x: x if x not in ['应届生', '不限', '其它工作经验'] else '0'
        )
        train_data['salary'] = train_data['salary'].astype('int')
        train_data = train_data[(train_data['salary'] > 1000)]
        train_data.to_csv('train.csv', index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    PATH = '../dataset/job.csv'
    data = pd.read_csv(PATH)
    data_clean = DataCleaning(data)
    data_clean.basic_cleaning()
    data_clean.format()
    data_clean.save()
