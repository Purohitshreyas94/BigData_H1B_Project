--5) Find the most popular top 10 job positions for H1B visa applications for each year?
--b) for only certified applications.

--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS (s_no,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


job = FOREACH h1b GENERATE year,job_title,case_status;

jobposistions = FILTER job BY case_status == 'CERTIFIED';

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


Top10JobPosistionsyear = UNION  top10jobposistions2011, top10jobposistions2012, top10jobposistions2013, top10jobposistions2014, top10jobposistions2015, top10jobposistions2016;

DUMP Top10JobPosistionsyear;
--(2015,PROGRAMMER ANALYST,48203)
--(2015,SOFTWARE ENGINEER,23352)
--(2015,COMPUTER PROGRAMMER,12971)
--(2015,SYSTEMS ANALYST,11498)
--(2015,SOFTWARE DEVELOPER,9343)
--(2015,TECHNOLOGY LEAD - US,8238)
--(2015,BUSINESS ANALYST,7919)
--(2015,COMPUTER SYSTEMS ANALYST,7234)
--(2015,TECHNOLOGY ANALYST - US,7009)
--(2015,SENIOR SOFTWARE ENGINEER,5324)
--(2014,PROGRAMMER ANALYST,38625)
--(2014,SOFTWARE ENGINEER,17278)
--(2014,COMPUTER PROGRAMMER,13796)
--(2014,SYSTEMS ANALYST,9161)
--(2014,BUSINESS ANALYST,6529)
--(2014,SOFTWARE DEVELOPER,6473)
--(2014,COMPUTER SYSTEMS ANALYST,6204)
--(2014,TECHNOLOGY LEAD - US,5055)
--(2014,TECHNOLOGY ANALYST - US,4911)
--(2014,SENIOR CONSULTANT,4535)
--(2011,PROGRAMMER ANALYST,28806)
--(2011,SOFTWARE ENGINEER,11224)
--(2011,COMPUTER PROGRAMMER,8038)
--(2011,SYSTEMS ANALYST,7850)
--(2011,BUSINESS ANALYST,3444)
--(2011,COMPUTER SYSTEMS ANALYST,3152)
--(2011,ASSISTANT PROFESSOR,3050)
--(2011,PHYSICAL THERAPIST,2911)
--(2011,SENIOR SOFTWARE ENGINEER,2595)
--(2011,SENIOR CONSULTANT,2585)
--(2013,PROGRAMMER ANALYST,29906)
--(2013,SOFTWARE ENGINEER,12973)
--(2013,COMPUTER PROGRAMMER,10202)
--(2013,SYSTEMS ANALYST,7850)
--(2013,TECHNOLOGY LEAD - US,7809)
--(2013,TECHNOLOGY ANALYST - US,7641)
--(2013,BUSINESS ANALYST,4993)
--(2013,COMPUTER SYSTEMS ANALYST,4554)
--(2013,SOFTWARE DEVELOPER,4316)
--(2013,SENIOR CONSULTANT,3996)
--(2016,PROGRAMMER ANALYST,47964)
--(2016,SOFTWARE ENGINEER,25890)
--(2016,SOFTWARE DEVELOPER,12474)
--(2016,SYSTEMS ANALYST,10986)
--(2016,COMPUTER PROGRAMMER,10528)
--(2016,BUSINESS ANALYST,8175)
--(2016,COMPUTER SYSTEMS ANALYST,6205)
--(2016,DEVELOPER,5912)
--(2016,SENIOR SOFTWARE ENGINEER,5630)
--(2016,TECHNOLOGY LEAD - US,5405)
--(2012,PROGRAMMER ANALYST,29226)
--(2012,SOFTWARE ENGINEER,12273)
--(2012,COMPUTER PROGRAMMER,8483)
--(2012,SYSTEMS ANALYST,8399)
--(2012,BUSINESS ANALYST,4144)
--(2012,COMPUTER SYSTEMS ANALYST,4084)
--(2012,SENIOR CONSULTANT,3420)
--(2012,SOFTWARE DEVELOPER,3290)
--(2012,PHYSICAL THERAPIST,3284)
--(2012,ASSISTANT PROFESSOR,3033)




