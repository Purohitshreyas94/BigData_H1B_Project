
--Q2 b) find top 5 locations in the US who have got certified visa for each year.[certified]

--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS
(s_no,case_status:chararray,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);

worksites = FOREACH h1b GENERATE year,case_status,worksite;

--DESCRIBE worksites;
--worksites: {year: chararray,case_status: chararray,worksite: chararray}

filter2011 = FILTER worksites BY year == '2011' AND case_status == 'CERTIFIED';

--DESCRIBE filter2011;
--filter2011: {year: chararray,case_status: chararray,worksite: chararray}


group2011 = GROUP filter2011 BY (year,worksite);

--DESCRIBE group2011;
--group2011: {group: (year: chararray,worksite: chararray),filter2011: {(year: chararray,case_status: chararray,worksite: chararray)}}


worksite2011 = FOREACH group2011 GENERATE FLATTEN(group) AS (year,worksite), COUNT(filter2011) AS certifiedcount;

--DESCRIBE worksite2011;
--worksite2011: {year: chararray,worksite: chararray,certifiedcount: long}


top5worksite2011 = LIMIT (ORDER worksite2011 BY $2 DESC) 5;

--DESCRIBE top5worksite2011;
--top5worksite2011: {year: chararray,worksite: chararray,certifiedcount: long}


--DUMP top5worksite2011;
--(2011,NEW YORK, NEW YORK,23172)
--(2011,HOUSTON, TEXAS,8184)
--(2011,CHICAGO, ILLINOIS,5188)
--(2011,SAN JOSE, CALIFORNIA,4713)
--(2011,SAN FRANCISCO, CALIFORNIA,4711)

-----------------------------------------------------------------------------------------------------------------------------------------------

filter2012 = FILTER worksites BY year == '2012' AND case_status == 'CERTIFIED';

group2012 = GROUP filter2012 BY (year,worksite);

worksite2012 = FOREACH group2012 GENERATE FLATTEN(group) AS (year,worksite), COUNT(filter2012) AS certifiedcount;

top5worksite2012 = LIMIT (ORDER worksite2012 BY $2 DESC) 5;

--DUMP top5worksite2012;
--(2012,NEW YORK, NEW YORK,23737)
--(2012,HOUSTON, TEXAS,9963)
--(2012,SAN FRANCISCO, CALIFORNIA,6116)
--(2012,CHICAGO, ILLINOIS,5671)
--(2012,ATLANTA, GEORGIA,5565)


-----------------------------------------------------------------------------------------------------------------------------------------------

filter2013 = FILTER worksites BY year == '2013' AND case_status == 'CERTIFIED';

group2013 = GROUP filter2013 BY (year,worksite);

worksite2013 = FOREACH group2013 GENERATE FLATTEN(group) AS (year,worksite), COUNT(filter2013) AS certifiedcount;

top5worksite2013 = LIMIT (ORDER worksite2013 BY $2 DESC) 5;

--DUMP top5worksite2013;
--(2013,NEW YORK, NEW YORK,23537)
--(2013,HOUSTON, TEXAS,11136)
--(2013,SAN FRANCISCO, CALIFORNIA,7281)
--(2013,SAN JOSE, CALIFORNIA,6722)
--(2013,ATLANTA, GEORGIA,6377)

-----------------------------------------------------------------------------------------------------------------------------------------------

filter2014 = FILTER worksites BY year == '2014' AND case_status == 'CERTIFIED';

group2014 = GROUP filter2014 BY (year,worksite);

worksite2014 = FOREACH group2014 GENERATE FLATTEN(group) AS (year,worksite), COUNT(filter2014) AS certifiedcount;

top5worksite2014 = LIMIT (ORDER worksite2014 BY $2 DESC) 5;

--DUMP top5worksite2014;
--(2014,NEW YORK, NEW YORK,27634)
--(2014,HOUSTON, TEXAS,13360)
--(2014,SAN FRANCISCO, CALIFORNIA,9798)
--(2014,SAN JOSE, CALIFORNIA,8223)
--(2014,ATLANTA, GEORGIA,8213)

----------------------------------------------------------------------------------------------------------------------------------------------

filter2015 = FILTER worksites BY year == '2015' AND case_status == 'CERTIFIED';

group2015 = GROUP filter2015 BY (year,worksite);

worksite2015 = FOREACH group2015 GENERATE FLATTEN(group) AS (year,worksite), COUNT(filter2015) AS certifiedcount;

top5worksite2015 = LIMIT (ORDER worksite2015 BY $2 DESC) 5;

--DUMP top5worksite2015;
--(2015,NEW YORK, NEW YORK,31266)
--(2015,HOUSTON, TEXAS,15242)
--(2015,SAN FRANCISCO, CALIFORNIA,12594)
--(2015,ATLANTA, GEORGIA,10500)
--(2015,SAN JOSE, CALIFORNIA,9589)

--------------------------------------------------------------------------------------------------------------------------------------------

filter2016 = FILTER worksites BY year == '2016' AND case_status == 'CERTIFIED';

group2016 = GROUP filter2016 BY (year,worksite);

worksite2016 = FOREACH group2016 GENERATE FLATTEN(group) AS (year,worksite), COUNT(filter2016) AS certifiedcount;

top5worksite2016 = LIMIT (ORDER worksite2016 BY $2 DESC) 5;

--DUMP top5worksite2016;
--(2016,NEW YORK, NEW YORK,34639)
--(2016,SAN FRANCISCO, CALIFORNIA,13836)
--(2016,HOUSTON, TEXAS,13655)
--(2016,ATLANTA, GEORGIA,11678)
--(2016,CHICAGO, ILLINOIS,11064)

---------------------------------------------------------------------------------------------------------------------------------------------

Top5LocationsInUsGotCertifiedVisaYear = UNION top5worksite2011,top5worksite2012,top5worksite2013,top5worksite2014,top5worksite2015,top5worksite2016;

--STORE Top5LocationsInUsGotCertifiedVisaYear INTO '/niit3/h1b/Pig/Q2B_Top5LocationsInUsGotCertifiedVisaYear';

--DESCRIBE Top5LocationsInUsGotCertifiedVisaYear;
--finaloutput = ORDER Top5LocationsInUsGotCertifiedVisaYear BY year

DUMP Top5LocationsInUsGotCertifiedVisaYear;
--(2011,NEW YORK, NEW YORK,23172)
--(2011,HOUSTON, TEXAS,8184)
--(2011,CHICAGO, ILLINOIS,5188)
--(2011,SAN JOSE, CALIFORNIA,4713)
--(2011,SAN FRANCISCO, CALIFORNIA,4711)
--(2013,NEW YORK, NEW YORK,23537)
--(2013,HOUSTON, TEXAS,11136)
--(2013,SAN FRANCISCO, CALIFORNIA,7281)
--(2013,SAN JOSE, CALIFORNIA,6722)
--(2013,ATLANTA, GEORGIA,6377)
--(2014,NEW YORK, NEW YORK,27634)
--(2014,HOUSTON, TEXAS,13360)
--(2014,SAN FRANCISCO, CALIFORNIA,9798)
--(2014,SAN JOSE, CALIFORNIA,8223)
--(2014,ATLANTA, GEORGIA,8213)
--(2015,NEW YORK, NEW YORK,31266)
--(2015,HOUSTON, TEXAS,15242)
--(2015,SAN FRANCISCO, CALIFORNIA,12594)
--(2015,ATLANTA, GEORGIA,10500)
--(2015,SAN JOSE, CALIFORNIA,9589)
--(2016,NEW YORK, NEW YORK,34639)
--(2016,SAN FRANCISCO, CALIFORNIA,13836)
--(2016,HOUSTON, TEXAS,13655)
--(2016,ATLANTA, GEORGIA,11678)
--(2016,CHICAGO, ILLINOIS,11064)
--(2012,NEW YORK, NEW YORK,23737)
--(2012,HOUSTON, TEXAS,9963)
--(2012,SAN FRANCISCO, CALIFORNIA,6116)
--(2012,CHICAGO, ILLINOIS,5671)
--(2012,ATLANTA, GEORGIA,5565)


