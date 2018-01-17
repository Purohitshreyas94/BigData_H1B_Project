

SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS prewage FROM h1b_final WHERE year = '2011' AND full_time_position = 'Y' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY prewage DESC LIMIT 5;


SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS prewage FROM h1b_final WHERE year = '2012' AND full_time_position = 'Y' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY prewage DESC LIMIT 5;



SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS prewage FROM h1b_final WHERE year = '2013' AND full_time_position = 'Y' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY prewage DESC LIMIT 5;



SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS prewage FROM h1b_final WHERE year = '2014' AND full_time_position = 'Y' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY prewage DESC LIMIT 5;



SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS prewage FROM h1b_final WHERE year = '2015' AND full_time_position = 'Y' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY prewage DESC LIMIT 5;



SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS prewage FROM h1b_final WHERE year = '2016' AND full_time_position = 'Y' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY prewage DESC LIMIT 5;



--Output--
--year	case_status	job_title	full_time_position	prewage
--2011	CERTIFIED-WITHDRAWN	AREA MANAGER, PHARMACEUTICAL PACKAGING	Y	2.1298784E8
--2011	CERTIFIED-WITHDRAWN	DEVELOPER (SOFTWARE SYSTEMS APPLICATIONS)	Y1.765608E8
--2011	CERTIFIED	SYSTEMS ENGINEER (DIAGNOSTICS)	Y	9.552608E7
--2011	CERTIFIED	SENIOR COST CONSULTANT	Y	8.561904E7
--2011	CERTIFIED	RADIATION ONCOLOGIST	Y	7.660264025E7



--year	case_status	job_title	full_time_position	prewage
--2012	CERTIFIED	QA COORDINATOR	Y	2.36785424E7
--2012	CERTIFIED	SAS PROGRAMMER	Y	1310280.9428571428
--2012	CERTIFIED	LECTURER IN HORTICULTURE	Y	693360.0
--2012	CERTIFIED	CARDIOLOGIST / ELECTROPHYSIOLOGIST	Y	475780.0
--2012	CERTIFIED	TEACHER, DEAF & HARD OF HEARING	Y	473773.0



--year	case_status	job_title	full_time_position	prewage
--2013	CERTIFIED	STAFF CONSULTANT - MICRO	Y	1.6950752E8
--2013	CERTIFIED	QA ANALYST/ PROGRAMMER	Y	7.0069927E7
--2013	CERTIFIED-WITHDRAWN	SOFTWARE QUALITY ASSURANCE ENGINEER	Y	2168876.1666666665
--2013	CERTIFIED	SOFTWARE PROJ. MGR./ARCHITECT	Y	891072.0
--2013	CERTIFIED	CLINICAL FELLOW, MINIMALLY INVASIVE SURGERY	Y	590913.0



--year	case_status	job_title	full_time_position	prewage
--2014	CERTIFIED	GASTROENTEROLOGIST PHYSICIAN	Y	631920.0
--2014	CERTIFIED	PHYSICIAN/NEUROSURGEON	Y	523713.0
--2014	CERTIFIED	MEDICAL ONCOLOGY PHYSICIAN	Y	483052.0
--2014	CERTIFIED-WITHDRAWN	GENERAL LEDGER ACCOUNTANT	Y	481476.0
--2014	CERTIFIED	PHYSICIAN CARDIOLOGIST	Y	467771.0



--year	case_status	job_title	full_time_position	prewage
--2015	CERTIFIED	MANAGER, GEORGIAN, CAUCASUS, AND EASTERN EUROPE REGIONAL MAN	Y	1.2308608E8
--2015	CERTIFIED	ENGINEERING QUALITY ANALYST (15-1199.0)	Y	9.993984E7
--2015	CERTIFIED	SR. MANAGER, SOX & INTERNAL AUDIT GROUP	Y	9.06598055E7
--2015	CERTIFIED	CHIEF EXECUTIVE OFFICER (CEO)	Y	453870.5
--2015	CERTIFIED	PHYSICIAN (GENERAL SURGERY)	Y	356900.0



--year	case_status	job_title	full_time_position	prewage
--2016	CERTIFIED	SYSTEMS ANALYSTS	Y	4869855.578947368
--2016	CERTIFIED	CARDIOLOGIST/INTERVENTIONAL CARDIOLOGIST	Y	350000.0
--2016	CERTIFIED	CARDIOLOGIST PHYSICIAN	Y	337800.0
--2016	CERTIFIED-WITHDRAWN	TRAUMA & GENERAL SURGEON	Y	328972.0
--2016	CERTIFIED-WITHDRAWN	ORTHOPEDIC SURGEON	Y	327529.0


















