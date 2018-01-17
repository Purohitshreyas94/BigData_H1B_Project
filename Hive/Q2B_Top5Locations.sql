
select year, worksite,count(*) as status from h1b_final where case_status='CERTIFIED' and year='2011' group by worksite,year order by status desc limit 5;
