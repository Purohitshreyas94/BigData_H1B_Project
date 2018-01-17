
--Q.7)) Create a bar graph to depict the number of applications for each year [All]

-->

select year, count(*) as applications from h1b_final group by year;

--year	applications
--2011	358767
--2013	442114
--2015	618727
--2012	415607
--2014	519427
--2016	647803

