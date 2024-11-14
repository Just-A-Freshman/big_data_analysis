# 1、数据集说明

这是一份来自 Johns Hopkins University 在github 开源的全球新冠肺炎 [COVID-19](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series) 数据集，每日时间序列汇总，包括确诊、死亡和治愈。所有数据来自每日病例报告。数据持续更新中。

# 2、 理解数据

通过搜索了解每个文件夹中数据的含义，见:

[datasetMeaning](./dataset/dataset_meaning.md)

# 3、 文件夹解释
process文件夹中包含了数据处理的代码, 其中:

time_series_analysis是对dataset/csse_covid_19_data/csse_covid_19_time_series_xx数据的分析代码

reports_file_analysis是对dataset/csse_covid_19_data/csse_covid_19_daily_reports_xx数据的聚合代码

discard是废弃代码，是在初期分析时使用到的代码，后期没有使用到。

# 4、 分析思路
[analysis_process](./dataset/target_task.md)

