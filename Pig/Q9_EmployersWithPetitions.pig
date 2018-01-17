
h1b = LOAD '/user/hive/warehouse/h1bproject.db/h1b_final' USING PigStorage('\t') AS 
(s_no,case_status,employer_name,soc_name,job_title:chararray,full_time_position,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);


--DESCRIBE h1b;
--h1b: {s_no: bytearray,case_status: bytearray,employer_name: bytearray,soc_name: bytearray,job_title: chararray,full_time_position: bytearray,prevailing_wage: long,year: chararray,worksite: chararray,longitute: double,latitute: double}


--DUMP h1b;

petitions = FOREACH h1b GENERATE case_status,employer_name; 
	

certified = FILTER petitions BY case_status == 'CERTIFIED';


withdrawn = FILTER petitions BY case_status == 'CERTIFIED-WITHDRAWN';


grp1 =  GROUP petitions BY employer_name;

grp2 = GROUP certified BY employer_name;

grp3 = GROUP withdrawn BY employer_name;


count1 = FOREACH grp1 GENERATE group as Emp_name, COUNT(petitions);

count2 = FOREACH grp2 GENERATE group as Emp_name, COUNT(certified);

count3 = FOREACH grp3 GENERATE group as Emp_name, COUNT(withdrawn);


joined = JOIN count1 BY $0, count2 BY $0, count3 BY $0;

--DESCRIBE joined;
               
                    
--joined: {count1::Emp_name: bytearray,long,count2::Emp_name: bytearray,long,count3::Emp_name: bytearray,long}

success = FOREACH joined GENERATE $0,$1,($3+$5);

success1 = FOREACH success GENERATE $0,$1,((float)$2*100/(float)$1);

rate = FILTER success BY $1 > 1000 AND $2 > 70;

successrate = LIMIT (ORDER rate BY $2 DESC)5;


--STORE finalsuccessrate INTO '/H1B_Project/Pig/Q9_EmployersWithTheNumberOfPetitions'

DUMP successrate;

--Output--
--(INFOSYS LIMITED,130592,99.54055)
--(ACCENTURE LLP,33447,99.39307)
--(TATA CONSULTANCY SERVICES LIMITED,64726,99.337204)
--(HCL AMERICA, INC.,22678,99.26801)
--(RELIABLE SOFTWARE RESOURCES, INC.,1992,99.14658)







