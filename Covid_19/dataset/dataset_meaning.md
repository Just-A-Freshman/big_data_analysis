# dataset meaning

## archived_data
The data is statistical data from the early stages of the pandemic, which was later standardized and moved to the csse_covid_19_data folder and modified for format consistency. This folder is for data archiving only and will not be maintained or updated moving forward.

## who_covid_19_situation_reports
This is data compiled by the World Health Organization. Due to the different definitions of case confirmation used by the WHO, the data they provide significantly differs from the data in the csse_covid_19_data folder. To provide a more comprehensive view of the COVID-19 situation, the repository author has added this folder. In short, this is external data compiled by non-authors and requires separate download. No additional analysis will be conducted here.

## csse_covid_19_data
- csse_covid_19_daily_reports: 
The data provided includes coarse-grained statistics for countries around the world, including the United States.

- csse_covid_19_daily_reports_us: The data provides detailed granularity for individual states within the United States.

- csse_covid_19_time_series: The data aggregates information from both ①csse_covid_19_daily_reports and ②csse_covid_19_daily_reports_us, offering only the aggregation results at the national level. In theory, we could compile this file ourselves based on ① and ②. For the data in this folder, we can analyze it directly or attempt to aggregate ① and ② for comparison.

- UID_ISO_FIPS_LookUp_Table.csv