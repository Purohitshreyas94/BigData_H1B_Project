--3)Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]

REGISTER /home/hduser/udfpig.jar;

DEFINE TitleContains Search();

--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,employer_name,soc_name:chararray,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


industry = FOREACH h1b GENERATE case_status,soc_name,job_title;

--DESCRIBE industry;
--industry: {case_status: chararray,soc_name: chararray,job_title: chararray}


filtered = FILTER industry BY case_status == 'CERTIFIED' AND TitleContains(job_title, 'DATA SCIENTIST');

--DESCRIBE filtered;                                                                                  
--filtered: {case_status: chararray,soc_name: chararray,job_title: chararray}

grouped = GROUP filtered BY soc_name;

--DESCRIBE grouped;
--grouped: {group: chararray,filtered: {(case_status: chararray,soc_name: chararray,job_title: chararray)}}

datascientist = FOREACH grouped GENERATE group AS industryname,COUNT(filtered) AS noposistions;

--DESCRIBE datascientist;                                                                        
--datascientist: {industryname: chararray,noposistions: long}


mostdatascientist = LIMIT (ORDER datascientist BY $1 DESC) 1;

--DESCRIBE mostdatascientist;                             
--mostdatascientist: {industryname: chararray,noposistions: long}

DUMP mostdatascientist;
--(STATISTICIANS,572)




