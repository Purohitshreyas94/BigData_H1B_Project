--5) Find the most popular top 10 job positions for H1B visa applications for each year?
--a) for all the applications

--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


jobposistions = FOREACH h1b GENERATE year,job_title;

filter2011 = FILTER jobposistions BY year == '2011';

group2011 = GROUP filter2011 BY (year,job_title);

mostpopularjob2011 = FOREACH group2011 GENERATE FLATTEN(group) AS (year,job_title),COUNT(filter2011) AS total;

top10jobposistions2011 = LIMIT (ORDER mostpopularjob2011 BY $2 DESC) 10;

--DUMP top10jobposistions2011;



filter2012 = FILTER jobposistions BY year == '2012';

group2012 = GROUP filter2012 BY (year,job_title);

mostpopularjob2012 = FOREACH group2012 GENERATE FLATTEN(group) AS (year,job_title),COUNT(filter2012) AS total;

top10jobposistions2012 = LIMIT (ORDER mostpopularjob2012 BY $2 DESC) 10;


filter2013 = FILTER jobposistions BY year == '2013';

group2013 = GROUP filter2013 BY (year,job_title);

mostpopularjob2013 = FOREACH group2013 GENERATE FLATTEN(group) AS (year,job_title),COUNT(filter2013) AS total;

top10jobposistions2013 = LIMIT (ORDER mostpopularjob2013 BY $2 DESC) 10;


filter2014 = FILTER jobposistions BY year == '2014';

group2014 = GROUP filter2014 BY (year,job_title);

mostpopularjob2014 = FOREACH group2014 GENERATE FLATTEN(group) AS (year,job_title),COUNT(filter2014) AS total;

top10jobposistions2014 = LIMIT (ORDER mostpopularjob2014 BY $2 DESC) 10;



filter2015 = FILTER jobposistions BY year == '2015';

group2015 = GROUP filter2015 BY (year,job_title);

mostpopularjob2015 = FOREACH group2015 GENERATE FLATTEN(group) AS (year,job_title),COUNT(filter2015) AS total;

top10jobposistions2015 = LIMIT (ORDER mostpopularjob2015 BY $2 DESC) 10;


filter2016 = FILTER jobposistions BY year == '2016';

group2016 = GROUP filter2016 BY (year,job_title);

mostpopularjob2016 = FOREACH group2016 GENERATE FLATTEN(group) AS (year,job_title),COUNT(filter2016) AS total;

top10jobposistions2016 = LIMIT (ORDER mostpopularjob2016 BY $2 DESC) 10;


Top5JobPosistionsyear = UNION  top10jobposistions2011, top10jobposistions2012, top10jobposistions2013, top10jobposistions2014, top10jobposistions2015, top10jobposistions2016;

DUMP Top5JobPosistionsyear;

--(2015,PROGRAMMER ANALYST,53436)
--(2015,SOFTWARE ENGINEER,27259)
--(2015,COMPUTER PROGRAMMER,14054)
--(2015,SYSTEMS ANALYST,12803)
--(2015,SOFTWARE DEVELOPER,10441)
--(2015,BUSINESS ANALYST,8853)
--(2015,TECHNOLOGY LEAD - US,8242)
--(2015,COMPUTER SYSTEMS ANALYST,7918)
--(2015,TECHNOLOGY ANALYST - US,7014)
--(2015,SENIOR SOFTWARE ENGINEER,6013)
--(2013,PROGRAMMER ANALYST,33880)
--(2013,SOFTWARE ENGINEER,15680)
--(2013,COMPUTER PROGRAMMER,11271)
--(2013,SYSTEMS ANALYST,8714)
--(2013,TECHNOLOGY LEAD - US,7853)
--(2013,TECHNOLOGY ANALYST - US,7683)
--(2013,BUSINESS ANALYST,5716)
--(2013,COMPUTER SYSTEMS ANALYST,5043)
--(2013,SOFTWARE DEVELOPER,5026)
--(2013,SENIOR CONSULTANT,4326)
--(2014,PROGRAMMER ANALYST,43114)
--(2014,SOFTWARE ENGINEER,20500)
--(2014,COMPUTER PROGRAMMER,14950)
--(2014,SYSTEMS ANALYST,10194)
--(2014,SOFTWARE DEVELOPER,7337)
--(2014,BUSINESS ANALYST,7302)
--(2014,COMPUTER SYSTEMS ANALYST,6821)
--(2014,TECHNOLOGY LEAD - US,5057)
--(2014,TECHNOLOGY ANALYST - US,4913)
--(2014,SENIOR CONSULTANT,4898)
--(2011,PROGRAMMER ANALYST,31799)
--(2011,SOFTWARE ENGINEER,12763)
--(2011,COMPUTER PROGRAMMER,8998)
--(2011,SYSTEMS ANALYST,8644)
--(2011,BUSINESS ANALYST,3891)
--(2011,COMPUTER SYSTEMS ANALYST,3698)
--(2011,ASSISTANT PROFESSOR,3467)
--(2011,PHYSICAL THERAPIST,3377)
--(2011,SENIOR SOFTWARE ENGINEER,2935)
--(2011,SENIOR CONSULTANT,2798)
--(2012,PROGRAMMER ANALYST,33066)
--(2012,SOFTWARE ENGINEER,14437)
--(2012,COMPUTER PROGRAMMER,9629)
--(2012,SYSTEMS ANALYST,9296)
--(2012,BUSINESS ANALYST,4752)
--(2012,COMPUTER SYSTEMS ANALYST,4706)
--(2012,SOFTWARE DEVELOPER,3895)
--(2012,PHYSICAL THERAPIST,3871)
--(2012,ASSISTANT PROFESSOR,3801)
--(2012,SENIOR CONSULTANT,3737)
--(2016,PROGRAMMER ANALYST,53743)
--(2016,SOFTWARE ENGINEER,30668)
--(2016,SOFTWARE DEVELOPER,14041)
--(2016,SYSTEMS ANALYST,12314)
--(2016,COMPUTER PROGRAMMER,11668)
--(2016,BUSINESS ANALYST,9167)
--(2016,COMPUTER SYSTEMS ANALYST,6900)
--(2016,SENIOR SOFTWARE ENGINEER,6439)
--(2016,DEVELOPER,6084)
--(2016,TECHNOLOGY LEAD - US,5410)



