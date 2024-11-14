# 1、数据集说明

这是一份来自 Johns Hopkins University 在github 开源的全球新冠肺炎 [COVID-19](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series) 数据集，每日时间序列汇总，包括确诊、死亡和治愈。所有数据来自每日病例报告。数据持续更新中。
>由于数据集中没有美国的治愈数据，所以在统计全球的现有确诊人员和治愈率的时候会有很大误差，代码里面先不做这个处理，期待数据集的完善。

# 2、 理解数据

通过搜索了解每个文件夹中数据的含义，见:

[datasetMeaning](./dataset/dataset_meaning.md)

# 3、 文件夹解释
process文件夹中包含了数据处理的代码, 其中:

time_series_analysis是对dataset/csse_covid_19_data/csse_covid_19_time_series_xx数据的分析代码

reports_file_analysis是对dataset/csse_covid_19_data/csse_covid_19_daily_reports_xx数据的聚合代码

discard是废弃代码，是在初期分析时使用到的代码，后期没有使用到。

# 4、 分析思路
[analysis_process](./target_task.md)

