--4)Which top 5 employers file the most petitions each year? - Case Status - ALL

--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


employer = FOREACH h1b GENERATE year,employer_name;

filter2011 = FILTER employer BY year == '2011';

group2011 = GROUP filter2011 BY (year,employer_name);

mostpetitions2011 = FOREACH group2011 GENERATE FLATTEN(group) AS (year,employer_name), COUNT(filter2011) AS petitions;

top5employers2011 = LIMIT (ORDER mostpetitions2011 BY $2 DESC) 5;

--DUMP top5employers2011;
--(2011,TATA CONSULTANCY SERVICES LIMITED,5416)
--(2011,MICROSOFT CORPORATION,4253)
--(2011,DELOITTE CONSULTING LLP,3621)
--(2011,WIPRO LIMITED,3028)
--(2011,COGNIZANT TECHNOLOGY SOLUTIONS U.S. CORPORATION,2721)


filter2012 = FILTER employer BY year == '2012';

group2012 = GROUP filter2012 BY (year,employer_name);

mostpetitions2012 = FOREACH group2012 GENERATE FLATTEN(group) AS (year,employer_name), COUNT(filter2012) AS petitions;

top5employers2012 = LIMIT (ORDER mostpetitions2012 BY $2 DESC) 5;

--DUMP top5employers2012;



filter2013 = FILTER employer BY year == '2013';

group2013 = GROUP filter2013 BY (year,employer_name);

mostpetitions2013 = FOREACH group2013 GENERATE FLATTEN(group) AS (year,employer_name), COUNT(filter2013) AS petitions;

top5employers2013 = LIMIT (ORDER mostpetitions2013 BY $2 DESC) 5;

--DUMP top5employers2013;



filter2014 = FILTER employer BY year == '2014';

group2014 = GROUP filter2014 BY (year,employer_name);

mostpetitions2014 = FOREACH group2014 GENERATE FLATTEN(group) AS (year,employer_name), COUNT(filter2014) AS petitions;

top5employers2014 = LIMIT (ORDER mostpetitions2014 BY $2 DESC) 5;

--DUMP top5employers2014;



filter2015 = FILTER employer BY year == '2015';

group2015 = GROUP filter2015 BY (year,employer_name);

mostpetitions2015 = FOREACH group2015 GENERATE FLATTEN(group) AS (year,employer_name), COUNT(filter2015) AS petitions;

top5employers2015 = LIMIT (ORDER mostpetitions2015 BY $2 DESC) 5;

--DUMP top5employers2015;



filter2016 = FILTER employer BY year == '2016';

group2016 = GROUP filter2016 BY (year,employer_name);

mostpetitions2016 = FOREACH group2016 GENERATE FLATTEN(group) AS (year,employer_name), COUNT(filter2016) AS petitions;

top5employers2016 = LIMIT (ORDER mostpetitions2016 BY $2 DESC) 5;

--DUMP top5employers2016;


Top5EmployersMostPetitionsYear = UNION top5employers2011, top5employers2012, top5employers2013, top5employers2014, top5employers2015, top5employers2016;

DUMP Top5EmployersMostPetitionsYear;
--(2011,TATA CONSULTANCY SERVICES LIMITED,5416)
--(2011,MICROSOFT CORPORATION,4253)
--(2011,DELOITTE CONSULTING LLP,3621)
--(2011,WIPRO LIMITED,3028)
--(2011,COGNIZANT TECHNOLOGY SOLUTIONS U.S. CORPORATION,2721)
--(2012,INFOSYS LIMITED,15818)
--(2012,WIPRO LIMITED,7182)
--(2012,TATA CONSULTANCY SERVICES LIMITED,6735)
--(2012,DELOITTE CONSULTING LLP,4727)
--(2012,IBM INDIA PRIVATE LIMITED,4074)
--(2016,INFOSYS LIMITED,25352)
--(2016,CAPGEMINI AMERICA INC,16725)
--(2016,TATA CONSULTANCY SERVICES LIMITED,13134)
--(2016,WIPRO LIMITED,10607)
--(2016,IBM INDIA PRIVATE LIMITED,9787)
--(2015,INFOSYS LIMITED,33245)
--(2015,TATA CONSULTANCY SERVICES LIMITED,16553)
--(2015,WIPRO LIMITED,12201)
--(2015,IBM INDIA PRIVATE LIMITED,10693)
--(2015,ACCENTURE LLP,9605)
--(2013,INFOSYS LIMITED,32223)
--(2013,TATA CONSULTANCY SERVICES LIMITED,8790)
--(2013,WIPRO LIMITED,6734)
--(2013,DELOITTE CONSULTING LLP,6124)
--(2013,ACCENTURE LLP,4994)
--(2014,INFOSYS LIMITED,23759)
--(2014,TATA CONSULTANCY SERVICES LIMITED,14098)
--(2014,WIPRO LIMITED,8365)
--(2014,DELOITTE CONSULTING LLP,7017)
--(2014,ACCENTURE LLP,5498)



