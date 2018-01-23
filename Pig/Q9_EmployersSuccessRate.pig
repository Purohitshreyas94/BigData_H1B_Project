
--h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage() AS 
--(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


finalh1b = FOREACH h1b GENERATE case_status,employer_name;

--describe finalh1b;
--finalh1b: {case_status: chararray,employer_name: chararray}


allgrouped = GROUP finalh1b BY employer_name;

--describe allgrouped;
--allgrouped: {group: chararray,finalh1b: {(case_status: chararray,employer_name: chararray)}}


allcount = FOREACH allgrouped GENERATE group as employer_name,COUNT(finalh1b.case_status) as totalapplicaion;

--describe allcount;
--allcount: {employer_name: chararray,totalapplication: long}


filterh1b = FILTER finalh1b BY case_status == 'CERTIFIED-WITHDRAWN' OR case_status == 'CERTIFIED';

successgrouped = GROUP filterh1b BY employer_name;

--describe successgrouped;
--successgrouped: {group: chararray,filterh1b: {(case_status: chararray,employer_name: chararray)}}

successcount = FOREACH successgrouped GENERATE group AS employer_name,COUNT(filterh1b.case_status) as totalsuccess;

--describe successcount;                                                                                             
--successcount: {employer_name: chararray,totalsuccess: long}


joined = JOIN allcount BY $0,successcount BY $0;


finalbag = FOREACH joined GENERATE $0,$1 as petitions, (FLOAT)(($3*100)/$1) AS successrate;
											
filtersuccessrate = FILTER finalbag BY $1 >= 1000 AND $2 > 70;

successrate= LIMIT (ORDER filtersuccessrate BY $2 DESC)5;


--STORE successrate INTO '/H1B_Project/Pig/Q9EmployersSuccessRate';


DUMP successrate;


--Output--
--(HTC GLOBAL SERVICES INC.,2632,100.0)
--(HTC GLOBAL SERVICES, INC.,1164,100.0)
--(TECH MAHINDRA (AMERICAS),INC.,10732,99.0)
--(YASH & LUJAN CONSULTING, INC.,1372,99.0)
--(YASH TECHNOLOGIES, INC.,2214,99.0)







