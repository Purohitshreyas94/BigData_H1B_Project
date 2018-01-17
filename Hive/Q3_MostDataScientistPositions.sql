

select soc_name, count(*) as scount from h1b_final where job_title like '%DATA SCIENTIST%' and case_status='CERTIFIED' group by soc_name order by scount desc limit 1;

--Output--
--soc_name	scount
--STATISTICIANS	572

