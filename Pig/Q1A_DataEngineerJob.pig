--1 a) Is the number of petitions with Data Engineer job title increasing over time?

REGISTER /home/hduser/udfpig.jar;

DEFINE TitleContains Search();

--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


jobtitle = FOREACH h1b GENERATE year,job_title;

finalh1b = FILTER jobtitle BY TitleContains(job_title, 'DATA ENGINEER');

filter2011 = FILTER finalh1b BY year == '2011';

group2011 = GROUP filter2011 BY year;

datajob2011 = FOREACH group2011 GENERATE group AS year, COUNT(filter2011) AS total1;


filter2012 = FILTER finalh1b BY year == '2012';

group2012 = GROUP filter2012 BY year;

datajob2012 = FOREACH group2012 GENERATE group AS year, COUNT(filter2012) AS total2;



filter2013 = FILTER finalh1b BY year == '2013';

group2013 = GROUP filter2013 BY year;

datajob2013 = FOREACH group2013 GENERATE group AS year, COUNT(filter2013) AS total3;


filter2014 = FILTER finalh1b BY year == '2014';

group2014 = GROUP filter2014 BY year;

datajob2014 = FOREACH group2014 GENERATE group AS year, COUNT(filter2014) AS total4;


filter2015 = FILTER finalh1b BY year == '2015';

group2015 = GROUP filter2015 BY year;

datajob2015 = FOREACH group2015 GENERATE group AS year, COUNT(filter2015) AS total5;


filter2016 = FILTER finalh1b BY year == '2016';

group2016 = GROUP filter2016 BY year;

datajob2016 = FOREACH group2016 GENERATE group AS year, COUNT(filter2016) AS total6;


final = FOREACH datajob2011 GENERATE datajob2011.$1,datajob2012.$1,datajob2013.$1,datajob2014.$1,datajob2015.$1,datajob2016.$1;

--DUMP final;
--(60,81,151,249,394,786)

finaloutput = FOREACH final GENERATE ROUND_TO((float)((($1-$0)*100)/$0),2), ROUND_TO((float)((($2-$1)*100)/$1),2), ROUND_TO((float)((($3-$2)*100)/$2),2), ROUND_TO((float)((($4-$3)*100)/$3),2), ROUND_TO((float)((($5-$4)*100)/$4),2);

--DUMP finaloutput;
--(35.0,86.42,64.9,58.23,99.49)

DataEnggJob = FOREACH finaloutput GENERATE ROUND_TO((float)(($0+$1+$2+$3+$4)/5),2) AS totalavg;

DUMP DataEnggJob;
--(68.4)








