

create table if not exists total_employers(employer_name string, total int)
row format delimited fields terminated by  '\t';

insert overwrite table total_employers select employer_name,count(*) as total  from h1b_final group by employer_name;


create table if not exists success_rate(employer_name string , success int) 
row format delimited fields terminated by  '\t';

insert overwrite table success_rate select employer_name,count(*) as success from h1b_final where case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by employer_name;


select a.employer_name ,a.total,b.success, ROUND((b.success/a.total)*100,2) as successrate from total_employers a, success_rate b where a.employer_name=b.employer_name AND a.total >= 1000 AND ROUND((b.success/a.total)*100,2) > 70 group by a.employer_name,a.total,b.success order by successrate desc limit 10;


--a.employer_name	a.total	b.success	successrate
--HTC GLOBAL SERVICES, INC.	1164	1164	100.0
--HTC GLOBAL SERVICES INC.	2632	2632	100.0
--INFOSYS LIMITED	130592	129992	99.54
--DIASPARK, INC.	1419	1412	99.51
--ACCENTURE LLP	33447	33244	99.39
--TECH MAHINDRA (AMERICAS),INC.	10732	10661	99.34
--TATA CONSULTANCY SERVICES LIMITED	64726	64297	99.34
--YASH TECHNOLOGIES, INC.	2214	2198	99.28
--YASH & LUJAN CONSULTING, INC.	1372	1362	99.27
--HCL AMERICA, INC.	22678	22512	99.27

