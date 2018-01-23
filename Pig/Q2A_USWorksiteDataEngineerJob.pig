
--2 a) Which part of the US has the most Data Engineer jobs for each year?[certified]


REGISTER /home/hduser/udfpig.jar;

DEFINE TitleContains Search();

--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);

jobtitle = FOREACH h1b GENERATE year,job_title,case_status,worksite;




filter1 = FILTER jobtitle BY year == '2011' AND TitleContains(job_title, 'DATA ENGINEER') AND case_status == 'CERTIFIED';


group1 = GROUP filter1 BY $3;

job2011 = FOREACH group1 GENERATE FLATTEN(filter1.year) AS year,group AS worksite, COUNT(filter1) AS total1;

topjob2011 = LIMIT (ORDER job2011 BY $2 DESC) 1;

--DUMP topjob2011;
--(SEATTLE, WASHINGTON,2011,19)

------------------------------------------------------------------------------------------------------------------------------------------

filter2 = FILTER jobtitle BY year matches '2012' AND TitleContains(job_title, 'DATA ENGINEER') AND TRIM(case_status) == 'CERTIFIED';

group2 = GROUP filter2 BY $3;

job2012 = FOREACH group2 GENERATE FLATTEN(filter2.year) AS year,group AS worksite, COUNT(filter2) AS total2;

topjob2012 = LIMIT (ORDER job2012 BY $2 DESC) 1;

---------------------------------------------------------------------------------------------------------------------------------

filter3 = FILTER jobtitle BY year matches '2013' AND TitleContains(job_title, 'DATA ENGINEER') AND TRIM(case_status) == 'CERTIFIED';

group3 = GROUP filter3 BY $3;

job2013 = FOREACH group3 GENERATE FLATTEN(filter3.year) AS year,group AS worksite, COUNT(filter3) AS total3;

topjob2013 = LIMIT (ORDER job2013 BY $2 DESC) 1;

---------------------------------------------------------------------------------------------------------------------------------

filter4 = FILTER jobtitle BY year matches '2014' AND TitleContains(job_title, 'DATA ENGINEER') AND TRIM(case_status) == 'CERTIFIED';

group4 = GROUP filter4 BY $3;

job2014 = FOREACH group4 GENERATE FLATTEN(filter4.year) AS year,group AS worksite, COUNT(filter4) AS total4;

topjob2014 = LIMIT (ORDER job2014 BY $2 DESC) 1;

---------------------------------------------------------------------------------------------------------------------------------

filter5 = FILTER jobtitle BY year matches '2015' AND TitleContains(job_title, 'DATA ENGINEER') AND TRIM(case_status) == 'CERTIFIED';

group5 = GROUP filter5 BY $3;

job2015 = FOREACH group5 GENERATE FLATTEN(filter5.year) AS year,group AS worksite, COUNT(filter5) AS total5;

topjob2015 = LIMIT (ORDER job2015 BY $2 DESC) 1;

---------------------------------------------------------------------------------------------------------------------------------

filter6 = FILTER jobtitle BY year matches '2016' AND TitleContains(job_title, 'DATA ENGINEER') AND TRIM(case_status) == 'CERTIFIED';

group6 = GROUP filter6 BY $3;

job2016 = FOREACH group6 GENERATE FLATTEN(filter6.year) AS year,group AS worksite, COUNT(filter6) AS total6;

topjob2016 = LIMIT (ORDER job2016 BY $2 DESC) 1;

---------------------------------------------------------------------------------------------------------------------------------


USWorksiteHasMostDataEnggJob = UNION topjob2011, topjob2012, topjob2013, topjob2014, topjob2015, topjob2016;

DUMP USWorksiteHasMostDataEnggJob;

--(2013,SEATTLE, WASHINGTON,43)
--(2015,SEATTLE, WASHINGTON,60)
--(2014,SEATTLE, WASHINGTON,42)
--(2016,SEATTLE, WASHINGTON,121)
--(2012,SEATTLE, WASHINGTON,26)
--(2011,SEATTLE, WASHINGTON,19)



