

h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage() AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


jobtitle = FOREACH h1b GENERATE year,job_title,case_status,worksite;

year1 = FILTER jobtitle BY $0 matches '2011';

group1 = GROUP year1 BY $1;

count1 = FOREACH group1 GENERATE group,COUNT($1);


year2 = FILTER jobtitle BY $0 matches '2012';

group2 = GROUP year2 BY $1;

count2 = FOREACH group2 GENERATE group,COUNT($1);



year3 = FILTER jobtitle BY $0 matches '2013';

group3 = GROUP year3 BY $1;

count3 = FOREACH group3 GENERATE group,COUNT($1);



year4 = FILTER jobtitle BY $0 matches '2014';

group4 = GROUP year4 BY $1;

count4 = FOREACH group4 GENERATE group,COUNT($1);



year5 = FILTER jobtitle BY $0 matches '2015';

group5 = GROUP year5 BY $1;

count5 = FOREACH group5 GENERATE group,COUNT($1);



year6 = FILTER jobtitle BY $0 matches '2016';

group6 = GROUP year6 BY $1;

count6 = FOREACH group6 GENERATE group,COUNT($1);

joined = JOIN count1 BY $0, count2 BY $0, count3 BY $0, count4 BY $0, count5 BY $0, count6 BY $0;

application = FOREACH joined GENERATE $0,$1,$3,$5,$7,$9,$11;

--growth = FOREACH joined GENERATE $0,ROUND_TO(($2-$1)*100/$1,2),ROUND_TO(($3-$2)*100/$2,2),ROUND_TO(($4-$3)*100/$3,2),ROUND_TO(($5-$4)*100/$4,2),ROUND_TO(($6-$5)*100/$5,2);


growth= FOREACH application  GENERATE $0,
(float)($6-$5)*100/$5,(float)($5-$4)*100/$4,
(float)($4-$3)*100/$3,(float)($3-$2)*100/$2,
(float)($2-$1)*100/$1;


avggrowth= FOREACH growth GENERATE $0,($1+$2+$3+$4+$5)/5;

top5jobtitle = LIMIT (ORDER avggrowth BY $1 DESC) 5;


--STORE top5jobtitle into '/H1B_Project/Pig/Q1B_Top5JobTitles';

DUMP top5jobtitle;

	
--(SENIOR SYSTEMS ANALYST JC60,4255.4644)
--(SOFTWARE DEVELOPER 2,3480.5925)
--(PROJECT MANAGER 3,3233.3335)
--(SYSTEMS ANALYST JC65,2984.8809)
--(MODULE LEAD,2917.112)


