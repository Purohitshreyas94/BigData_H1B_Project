
h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage('\t') AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


--DESCRIBE h1b;
--h1b: {s_no: bytearray,case_status: bytearray,employer_name: bytearray,soc_name: bytearray,job_title: chararray,full_time_position: bytearray,prevailing_wage: long,year: chararray,worksite: chararray,longitute: double,latitute: double}


--DUMP h1b;

petitions = FOREACH h1b GENERATE case_status,job_title; 


certified = FILTER petitions BY case_status == 'CERTIFIED';


withdrawn = FILTER petitions BY case_status == 'CERTIFIED-WITHDRAWN';


grp1 =  GROUP petitions BY job_title;

grp2 = GROUP certified BY job_title;

grp3 = GROUP withdrawn BY job_title;


count1 = FOREACH grp1 GENERATE group as Job_title, COUNT(petitions);

count2 = FOREACH grp2 GENERATE group as Job_title, COUNT(certified);

count3 = FOREACH grp3 GENERATE group as Job_title, COUNT(withdrawn);


joined = JOIN count1 BY $0, count2 BY $0, count3 BY $0;

--DESCRIBE joined;

--joined: {count1::Emp_name: bytearray,long,count2::Emp_name: bytearray,long,count3::Emp_name: bytearray,long}

total = FOREACH joined GENERATE $0,$1,($3+$5);

success = FOREACH total GENERATE $0,$1,(float)(($2)*100)/(float)$1;

successrate = FILTER success BY $1>=1000 AND $2>70;	

jobpositionssuccessrate = LIMIT (ORDER successrate BY $2 DESC)5;


--STORE jobpositionssuccessrate INTO '/H1B_Project/Pig/Q10_JobPositionsWithTheNumberOfPetitions';

DUMP jobpositionssuccessrate;

--Output--
--(COMPUTER PROGRAMMER / CONFIGURER 2,1276,100.0)
--(ASSOCIATE CONSULTANT - US,4393,99.93171)
--(SYSTEMS ENGINEER - US,10036,99.90036)
--(TEST ANALYST - US,4958,99.818474)
--(CONSULTANT - US,7426,99.81147)

