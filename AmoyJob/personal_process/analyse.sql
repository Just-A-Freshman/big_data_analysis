# 务必注意用``来引起来，这能影响排序结果
# hql主要是用在Java的，因此没必要去学，除非真要用

# 通过这句话确定建立前缀索引最合适的切片数字为7
SELECT
COUNT(DISTINCT LEFT(`position`, 7)) / COUNT(position) AS `repeat_rate`
FROM `amoy_job`;

-- 整体情况（招聘企业数、岗位数、招聘人数）
SELECT
    COUNT(DISTINCT `company`) AS `招聘企业数`,
    COUNT(1) AS `岗位数``,
    SUM(`num`) AS `招聘人数`,
FROM
    amoy_job;

# --平均薪资需要单独求
SELECT AVG(CAST(SUBSTRING_INDEX(`salary`, '-', 1) AS SIGNED)) AS `avg_lb_salary`
FROM amoy_job
WHERE salary` <> '面议';

-- 公司类型情况
CREATE VIEW company_type_avg_salary AS(
SELECT
	company_type,
	AVG(CAST(SUBSTRING_INDEX(salary, '-', 1)AS SIGNED)) `avg_salary`
FROM amoy_job
WHERE salary <> '面议'
GROUP BY company_type);

SELECT
	t1.company_type, SUM(t1.num) AS workers, t2.avg_salary
FROM amoy_job t1
JOIN company_type_avg_salary t2
ON t1.company_type = t2.company_type
GROUP BY t1.company_type;

-- 鉴于有太多的查询语句了，为了更具有效率，此处只再选取部分语句进行对应查询

--工作经验, 粗化分类
SELECT
	CASE
		 WHEN work_experience in ('应届生', '不限', '其他') THEN work_experience
		 WHEN work_experience rlike '^[一二三]年$' THEN '1-3年工作经验'
         WHEN work_experience rlike '^[四五]年$' THEN '4-5年工作经验'
         WHEN work_experience rlike '^[六七八九十]年$' THEN '6-10年工作经验'
		 ELSE '10年工作经验以上'
	END AS `工作经验` ,
    AVG(salary) `平均薪资`,
    SUM(num) `招聘人数`
FROM amoy_job
WHERE salary <> '面议'
GROUP BY `工作经验`;


-- 年龄(要简化还是各个阶段都统计进行累加?-选择了各个年龄段都统计，非常可视化的折线图趋势对比)
-- 众所周知的35岁危机
SELECT
	 CASE WHEN (
		CAST(SUBSTRING_INDEX(age, '-', -1) AS SIGNED)
        ) >= 35 THEN '接纳35岁的'
     ELSE '淘汰35岁的' END AS `年龄划分`,
     COUNT(1) AS `职位`,
     SUM(num) AS `招聘人数`
FROM amoy_job
WHERE
	age <> '不限'
GROUP BY `年龄划分`;

-- 技能: lateral view explode不指望了，因为我不选择使用hive SQL, 但这不妨碍我作出类似功能
-- 超级屎山，查询很慢但又不好优化

WITH languageRequire as(
	SELECT `require`, salary
    FROM amoy_job
    WHERE salary <> '面议'
)
SELECT 'Python' AS `编程语言要求`, AVG(salary) AS `平均薪资`
FROM languageRequire WHERE `require` LIKE '%Python%'
UNION ALL
SELECT 'C++', AVG(salary) FROM languageRequire WHERE `require` LIKE '%C++%'
UNION ALL
SELECT 'Php', AVG(salary) FROM languageRequire WHERE `require` LIKE '%Php%'
UNION ALL
SELECT 'SQL', AVG(salary) FROM languageRequire WHERE `require` LIKE '%SQL%'
UNION ALL
SELECT 'Go', AVG(salary) FROM languageRequire WHERE `require` LIKE '%Go%'
UNION ALL
SELECT 'C', AVG(salary) FROM languageRequire WHERE `require` LIKE '%C语言%';

-- 模型训练(导出为 train.csv)
-- 看一下训练目的，先从简单的训练目的开始, 如仅通过学历和工作年限来预测薪资
SELECT
	education `要求最低学历`,
    CASE WHEN work_experience IN ('应届生', '不限') THEN '无'
		 ELSE work_experience END `要求工作经验`,
    AVG(salary) `平均薪资`
FROM amoy_job
WHERE salary <> '面议'
AND education <> '不限'
AND work_experience <> '其他'
GROUP BY `要求最低学历`, `要求工作经验`;




