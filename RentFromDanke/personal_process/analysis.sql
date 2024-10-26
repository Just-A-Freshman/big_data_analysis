-- 查看总体情况，这里特别注意这个CASE WHEN的写法
SELECT 
	AVG(`价格`) `平均价格`,
	AVG(`面积`) `平均面积`,
	AVG(`所在楼层`) `平均楼层`,
    AVG(CASE WHEN `地铁距离` <> 65535 THEN `地铁距离` END) AS `平均地铁距离`,
    (SELECT `室` FROM rent GROUP BY `室` ORDER BY COUNT(`室`) DESC LIMIT 1) `室(最多)`,
    (SELECT `卫` FROM rent GROUP BY `卫` ORDER BY COUNT(`卫`) DESC LIMIT 1) `卫(最多)`,
    SUM(`价格`) / SUM(`面积`) `每平米租价`
FROM rent;

-- GROUP BY 查看平均地租与其他字段的关系
-- 平均地租最贵的10个小区
SELECT `小区`, SUM(`价格`) / SUM(`面积`) `平均地租`
FROM rent
GROUP BY `小区`
ORDER BY `平均地租` DESC
LIMIT 10;

-- 最常见的10种户型
-- 如果是1个人住的话，正常1室就够了，但是, 出租户型中，绝大部分是3室, 证明更有可能是合租
SELECT CONCAT(`室`, '室', `卫`, '卫') `户型`, COUNT(1), AVG(`面积`) `平均面积`
FROM rent GROUP BY `户型` LIMIT 10;

-- 电梯房与非电梯房的区分
SELECT 
	CASE WHEN `总楼层` > 7 THEN '电梯房' ELSE '非电梯房' END `类型`,
	COUNT(1) `数量`,
	AVG(`面积`) `平均面积`,
    SUM(`价格`) / SUM(`面积`) `平均地租`
FROM rent GROUP BY `类型`;

-- 在区分出楼梯房的基础上，进一步加入所在楼层的区分; 特别的，此时使用了嵌套的CASE WHEN

SELECT 
	CASE WHEN `总楼层` > 7 THEN '电梯房' ELSE '非电梯房' END `房子分类`,
    CASE WHEN `总楼层` > 7 
		THEN (CASE WHEN `所在楼层` < 10 THEN '低楼层' WHEN `所在楼层` > 19 THEN '高楼层' ELSE '中楼层' END) 
		ELSE (CASE WHEN `所在楼层` < 3 THEN '低楼层' WHEN `所在楼层` > 4 THEN '高楼层' ELSE '中楼层' END)
		END `楼层分类`,
    SUM(`价格`) / SUM(`面积`) `平均地租`,
    COUNT(1) `数量`
FROM rent
GROUP BY `房子分类`, `楼层分类`;

-- 地铁距离划分
SELECT 
	CASE WHEN `地铁距离` < 500 THEN '<500米'
		 WHEN `地铁距离` BETWEEN 500 AND 1000 THEN '500-1000米'
         WHEN `地铁距离` BETWEEN 1000 AND 1500 THEN '1000-1500米'
         ELSE '>1500米' END `地铁距离划分`,
	SUM(`价格`) / SUM(`面积`) `平均租价`,
    COUNT(1) `数量`
FROM rent
GROUP BY `地铁距离划分`;
