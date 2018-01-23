


mysql -u root -p'root' -e 'DROP DATABASE h1b_project;CREATE DATABASE IF NOT EXISTS h1b_project;USE h1b_project;CREATE TABLE h1b_final(job_title VARCHAR(255) NOT NULL,petitions INT NOT NULL,success_rate FLOAT NOT NULL);';



#sqoop list-tables --connect jdbc:mysql://localhost/h1b_project --username root --password root;


sqoop export --connect jdbc:mysql://localhost/h1b_project --username root --password 'root' --table h1b_final --update-mode  allowinsert --export-dir /H1B_Project/Pig/Q10JobPositionsSuccessRate/part-r-00000 --input-fields-terminated-by '\t' -m 1 ;

echo -e '\n\n Display content from MySQL Database.\n\n'
echo -e '\n 10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?\n\n'

mysql -u root -p'root' -e 'SELECT * FROM h1b_project.h1b_final';
