
h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage() AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,
worksite:chararray,longitute:double,latitute:double);



applications = FOREACH h1b GENERATE case_status, year; 


grp1 = GROUP applications BY year;


count1 = FOREACH grp1 GENERATE group as year, COUNT(applications);

--DUMP count1;
--(2011,358767)
--(2012,415607)
--(2013,442114)
--(2014,519427)
--(2015,618727)
--(2016,647803)


status = GROUP applications BY (year,case_status);


count2 = FOREACH status GENERATE group, group.year, COUNT(applications);


joined = JOIN  count2 BY $1,count1 BY $0;


--joined: {count2::group: (year: chararray,case_status: bytearray),count2::year: chararray,long,count1::year: chararray,long}


totalapplications = FOREACH joined GENERATE FLATTEN($0), (FLOAT)($2*100)/$4, $2;


--STORE totalapplications INTO '/H1B_Project/Pig/Q6CountOfTotalApplications';


DUMP totalapplications;

--(2011,DENIED,8.119476,29130)
--(2011,CERTIFIED,85.83175,307936)
--(2011,WITHDRAWN,2.8165913,10105)
--(2011,CERTIFIED-WITHDRAWN,3.2321813,11596)
--(2012,DENIED,5.075949,21096)
--(2012,CERTIFIED,84.856125,352668)
--(2012,WITHDRAWN,2.5805628,10725)
--(2012,CERTIFIED-WITHDRAWN,7.487362,31118)
--(2013,CERTIFIED-WITHDRAWN,8.014222,35432)
--(2013,WITHDRAWN,2.6214957,11590)
--(2013,CERTIFIED,86.61816,382951)
--(2013,DENIED,2.7461243,12141)
--(2014,CERTIFIED-WITHDRAWN,6.998096,36350)
--(2014,WITHDRAWN,3.086863,16034)
--(2014,CERTIFIED,87.624245,455144)
--(2014,DENIED,2.2907934,11899)
--(2015,DENIED,1.765399,10923)
--(2015,CERTIFIED,88.452255,547278)
--(2015,WITHDRAWN,3.1443594,19455)
--(2015,CERTIFIED-WITHDRAWN,6.6379843,41071)
--(2016,CERTIFIED,87.93507,569646)
--(2016,WITHDRAWN,3.3791137,21890)
--(2016,CERTIFIED-WITHDRAWN,7.269494,47092)
--(2016,DENIED,1.4163257,9175)




