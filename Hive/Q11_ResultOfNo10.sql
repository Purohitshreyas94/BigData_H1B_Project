
--In MySQL--

create database h1b_project;

use h1b_project;

CREATE TABLE h1b_final(
   job_title VARCHAR(255) NOT NULL,
   petitions INT NOT NULL,
   success_rate FLOAT NOT NULL);

-------------------------------------------------------------------------------------------------------------------------------------------------------


sqoop list-tables --connect jdbc:mysql://localhost/h1b_project --username root --password root;


sqoop export --connect jdbc:mysql://localhost/h1b_project --username root --password root --table h1b_final --update-mode  allowinsert --export-dir /H1B_Project/Pig/Q10JobPositionsSuccessRate/part-r-00000 --input-fields-terminated-by '\t' ;




--Output--

mysql> select * from h1b_final;
+------------------------------------+-----------+--------------+
| job_title                          | petitions | success_rate |
+------------------------------------+-----------+--------------+
| PROJECT MANAGER - US               |      7046 |           99 |
| PROGRAMMER ANALYST - II            |      3588 |           99 |
| PRODUCTION SUPPORT LEAD - US       |      1301 |          100 |
| COMPUTER PROGRAMMER / CONFIGURER 2 |      1276 |          100 |
| LEAD CONSULTANT - US               |      3402 |           99 |
+------------------------------------+-----------+--------------+


