

create table if not exists total_jobpositions(job_title string, total int)
row format delimited fields terminated by  '\t';

insert overwrite table total_jobpositions select job_title,count(*) as total  from h1b_final group by job_title;


create table if not exists jobsuccess_rate(job_title string , success int) 
row format delimited fields terminated by  '\t';

insert overwrite table jobsuccess_rate select job_title,count(*) as success from h1b_final where case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title;


select a.job_title ,a.total,b.success, ROUND((b.success/a.total)*100,2) as jobsuccessrate from total_jobpositions a, jobsuccess_rate b where a.job_title=b.job_title AND a.total >= 1000 AND ROUND((b.success/a.total)*100,2) > 70 group by a.job_title,a.total,b.success order by jobsuccessrate desc limit 10;

