
CREATE TABLE IF NOT EXISTS dataengg(year string, total int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE dataengg SELECT year,COUNT(*) FROM h1b_final WHERE job_title LIKE '%DATA ENGINEER%' GROUP BY year ORDER BY year;


SELECT * FROM dataengg;
dataengg.year	dataengg.total
2011	60
2012	81
2013	151
2014	249
2015	394
2016	786


SELECT year, ROUND(((total-LAG(total,1)OVER(ORDER BY year))*100)/LAG(total,1)OVER(ORDER BY year),2) AS growthpercent FROM dataengg;

OR 

SELECT year, ROUND(((total-LAG(total)OVER(ORDER BY year))*100)/LAG(total)OVER(ORDER BY year),2) AS growthpercent FROM dataengg;



CREATE TABLE IF NOT EXISTS dataenggpercent(year string, percent1 float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';


INSERT OVERWRITE TABLE dataenggpercent SELECT year, ROUND(((total-LAG(total)over(ORDER BY year))*100)/LAG(total)OVER(ORDER BY year),2) AS growthpercent FROM dataengg;


SELECT * FROM dataenggpercent;
dataenggpercent.year	dataenggpercent.percent1
2011	NULL
2012	35.0
2013	86.42
2014	64.9
2015	58.23
2016	99.49



SELECT ROUND(AVG(percent1),2) AS avggrowth FROM dataenggpercent;

avggrowth
68.81

