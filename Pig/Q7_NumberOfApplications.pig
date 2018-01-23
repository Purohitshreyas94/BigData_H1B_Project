

--h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage() AS
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS  
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,
worksite:chararray,longitute:double,latitute:double);



applications = FOREACH h1b GENERATE case_status, year; 


grp1 = GROUP applications BY year;


totalapplication = FOREACH grp1 GENERATE group as year, COUNT(applications);

--STORE totalapplication INTO '/H1B_Project/Pig/Q7NumberOfApplications';

DUMP totalapplication;


--Output--
--(2011,358767)
--(2012,415607)
--(2013,442114)
--(2014,519427)
--(2015,618727)
--(2016,647803)

