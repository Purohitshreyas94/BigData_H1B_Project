
Q.4)Which top 5 employers file the most petitions each year? - Case Status - ALL


select employer_name, year,count(*) as total  from h1b_final where year='2011' group by employer_name,year order by total desc limit 5; 
-->
--employer_name	year	total
--TATA CONSULTANCY SERVICES LIMITED	2011	5416
--MICROSOFT CORPORATION	2011	4253
--DELOITTE CONSULTING LLP	2011	3621
--WIPRO LIMITED	2011	3028
--COGNIZANT TECHNOLOGY SOLUTIONS U.S. CORPORATION	2011	2721



select employer_name, year,count(*) as total  from h1b_final where year='2012' group by employer_name,year order by total desc limit 5; 
-->
--employer_name	year	total
--INFOSYS LIMITED	2012	15818
--WIPRO LIMITED	2012	7182
--TATA CONSULTANCY SERVICES LIMITED	2012	6735
--DELOITTE CONSULTING LLP	2012	4727
--IBM INDIA PRIVATE LIMITED	2012	4074



select employer_name, year,count(*) as total  from h1b_final where year='2013' group by employer_name,year order by total desc limit 5; 
-->
--employer_name	year	total
--INFOSYS LIMITED	2013	32223
--TATA CONSULTANCY SERVICES LIMITED	2013	8790
--WIPRO LIMITED	2013	6734
--DELOITTE CONSULTING LLP	2013	6124
--ACCENTURE LLP	2013	4994


select employer_name, year,count(*) as total  from h1b_final where year='2014' group by employer_name,year order by total desc limit 5; 
-->
--employer_name	year	total
--INFOSYS LIMITED	2014	23759
--TATA CONSULTANCY SERVICES LIMITED	2014	14098
--WIPRO LIMITED	2014	8365
--DELOITTE CONSULTING LLP	2014	7017
--ACCENTURE LLP	2014	5498



select employer_name, year,count(*) as total  from h1b_final where year='2015' group by employer_name,year order by total desc limit 5;
-->
--employer_name	year	total
--INFOSYS LIMITED	2015	33245
--TATA CONSULTANCY SERVICES LIMITED	2015	16553
--WIPRO LIMITED	2015	12201
--IBM INDIA PRIVATE LIMITED	2015	10693
--ACCENTURE LLP	2015	9605



select employer_name, year,count(*) as total  from h1b_final where year='2016' group by employer_name,year order by total desc limit 5;
-->
--employer_name	year	total
--INFOSYS LIMITED	2016	25352
--CAPGEMINI AMERICA INC	2016	16725
--TATA CONSULTANCY SERVICES LIMITED	2016	13134
--WIPRO LIMITED	2016	10607
--IBM INDIA PRIVATE LIMITED	2016	9787


------------------------------------------------------------------------------------------------------------------------------------------------------


