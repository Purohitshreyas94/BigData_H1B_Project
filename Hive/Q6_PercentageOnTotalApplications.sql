

create table if not exists total_application(total int,year string)
row format delimited 
fields terminated by ',';


insert overwrite table total_application select count(*),year from h1b_final where h1b_final.case_status is not NULL group by year;

                                                    
select a.case_status,count(*) as total,a.year,ROUND((count(*)/b.total)*100,2) as status from h1b_final a,total_application b  where a.year=b.year group by a.case_status,b.total,a.year order by a.year;


--select a.case_status,count(*) as total,a.year,ROUND((count(*)/b.total)*100,2) as status from h1b_final a left outer join total_application b on (a.year=b.year) where a.year is not NULL group by a.case_status,b.total,a.year order by a.year;




--Output--
--a.case_status	total	a.year	status
--WITHDRAWN	10105	2011	2.82
--DENIED	29130	2011	8.12
--CERTIFIED	307936	2011	85.83
--CERTIFIED-WITHDRAWN	11596	2011	3.23
--WITHDRAWN	10725	2012	2.58
--DENIED	21096	2012	5.08
--CERTIFIED-WITHDRAWN	31118	2012	7.49
--CERTIFIED	352668	2012	84.86
--WITHDRAWN	11590	2013	2.62
--CERTIFIED	382951	2013	86.62
--CERTIFIED-WITHDRAWN	35432	2013	8.01
--DENIED	12141	2013	2.75
--CERTIFIED	455144	2014	87.62
--CERTIFIED-WITHDRAWN	36350	2014	7.0
--WITHDRAWN	16034	2014	3.09
--DENIED	11899	2014	2.29
--WITHDRAWN	19455	2015	3.14
--CERTIFIED-WITHDRAWN	41071	2015	6.64
--DENIED	10923	2015	1.77
--CERTIFIED	547278	2015	88.45
--CERTIFIED	569646	2016	87.94
--DENIED	9175	2016	1.42
--WITHDRAWN	21890	2016	3.38
--CERTIFIED-WITHDRAWN	47092	2016	7.27

