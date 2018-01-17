

h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage() AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


finalh1b = FOREACH h1b GENERATE case_status,job_title;

--describe finalh1b;
--finalh1b: {case_status: chararray,job_title: chararray}


allgrouped = GROUP finalh1b BY job_title;

--describe allgrouped;
--allgrouped: {group: chararray,finalh1b: {(case_status: chararray,job_title: chararray)}}


allcount = FOREACH allgrouped GENERATE group as job_title,COUNT(finalh1b.case_status) as totalapplicaion;

--describe allcount;
--allcount: {job_title: chararray,totalapplication: long}


filterh1b = FILTER finalh1b BY case_status == 'CERTIFIED-WITHDRAWN' OR case_status == 'CERTIFIED';

successgrouped = GROUP filterh1b BY job_title;

--describe successgrouped;
--successgrouped: {group: chararray,filterh1b: {(case_status: chararray,job_title: chararray)}}

successcount = FOREACH successgrouped GENERATE group AS job_title,COUNT(filterh1b.case_status) as totalsuccess;

--describe successcount;                                                                                             
--successcount: {job_title: chararray,totalsuccess: long}


joined = JOIN allcount BY $0,successcount BY $0;


finalbag = FOREACH joined GENERATE $0,$1 as petitions, (FLOAT)(($3*100)/$1) AS successrate;
											
filtersuccessrate = FILTER finalbag BY $1 >= 1000 AND $2 > 70;

jobsuccessrate= LIMIT (ORDER filtersuccessrate BY $2 DESC)5;


--STORE jobsuccessrate INTO '/H1B_Project/Pig/Q10JobPositionsSuccessRate';


DUMP jobsuccessrate;

--Output--
--(PRODUCTION SUPPORT LEAD - US,1301,100.0)
--(COMPUTER PROGRAMMER / CONFIGURER 2,1276,100.0)
--(PROJECT MANAGER - US,7046,99.0)
--(LEAD CONSULTANT - US,3402,99.0)
--(PROGRAMMER ANALYST - II,3588,99.0)






