h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage() AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


--DESCRIBE h1b;
--h1b: {s_no: bytearray,case_status: bytearray,employer_name: bytearray,soc_name: bytearray,job_title: chararray,full_time_position: bytearray,prevailing_wage: long,year: chararray,worksite: chararray,longitute: double,latitute: double}


--DUMP h1b;

wage = FOREACH h1b GENERATE case_status,job_title,full_time_position,prevailing_wage,year; 

wage1 = FILTER wage BY case_status == 'CERTIFIED-WITHDRAWN' OR case_status == 'CERTIFIED';


wage2 = FILTER wage1 BY full_time_position == 'Y';


year2011 = FILTER wage2 BY year == '2011';

year2012 = FILTER wage2 BY year == '2012';

year2013 = FILTER wage2 BY year == '2013';

year2014 = FILTER wage2 BY year == '2014'; 

year2015 = FILTER wage2 BY year == '2015'; 

year2016 = FILTER wage2 BY year == '2016'; 


grp2011 = GROUP year2011 BY (year,case_status,job_title,prevailing_wage);

--DESCRIBE grp2011;
--{group: (year: chararray,case_status: bytearray,job_title: chararray,prevailing_wage: long),year2011: {(case_status: bytearray,job_title: chararray,full_time_position: bytearray,prevailing_wage: long,year: chararray)}}

grp2012 = GROUP year2012 BY (year,case_status,job_title,prevailing_wage);

grp2013 = GROUP year2013 BY (year,case_status,job_title,prevailing_wage);

grp2014 = GROUP year2014 BY (year,case_status,job_title,prevailing_wage);

grp2015 = GROUP year2015 BY (year,case_status,job_title,prevailing_wage);

grp2016 = GROUP year2016 BY (year,case_status,job_title,prevailing_wage);


avg2011 = FOREACH grp2011 GENERATE group , AVG(year2011.prevailing_wage);

avg2012 = FOREACH grp2012 GENERATE group , AVG(year2012.prevailing_wage);

avg2013 = FOREACH grp2013 GENERATE group , AVG(year2013.prevailing_wage);

avg2014 = FOREACH grp2014 GENERATE group , AVG(year2014.prevailing_wage);

avg2015 = FOREACH grp2015 GENERATE group , AVG(year2015.prevailing_wage);

avg2016 = FOREACH grp2016 GENERATE group , AVG(year2016.prevailing_wage);


AveragePrevailingWage = UNION avg2011, avg2012, avg2013, avg2014, avg2015, avg2016;


FullTimePrevailingWage = ORDER AveragePrevailingWage BY $0 DESC;


--STORE FullTimePrevailingWage INTO '/H1B_Project/Pig/Q8A_FullTimeAveragePrevailingWage';

DUMP FullTimePrevailingWage;




