#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue                              
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
        echo -e "${MENU}**********************ANALYSIS AND SUMMERIZATION OF H1B APPLICANTS***********************${NORMAL}"
    echo -e "${MENU}${NUMBER} 1A) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?(MR)(PIG)(HIVE)${NORMAL}"
    echo -e "${MENU}${NUMBER} 1B) ${MENU} Find top 5 job titles who are having highest growth in applications.(PIG)(MR)(HIVE) ${NORMAL}"
    echo -e "${MENU}${NUMBER} 2A) ${MENU} Which part of the US has the most Data Engineer jobs for each year?(HIVE)(MR)(PIG) ${NORMAL}"
    echo -e "${MENU}${NUMBER} 2B) ${MENU} Find top 5 locations in the US who have got certified visa for each year.(HIVE)(MR)(PIG)${NORMAL}"
    echo -e "${MENU}${NUMBER} 3) ${MENU} Which industry has the most number of Data Scientist positions?(MR)${NORMAL}(HIVE)(PIG)(MR)"
    echo -e "${MENU}${NUMBER} 4) ${MENU} Which top 5 employers file the most petitions each year?(MR) ${NORMAL}(MR)(HIVE)(PIG)"
    echo -e "${MENU}${NUMBER} 5A) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year?(HIVE)(MR)(PIG)${NORMAL}"
    echo -e "${MENU}${NUMBER} 5B) ${MENU} Find the most popular top 10 job positions for Certified H1B visa applications for each year?(HIVE)(MR)(PIG)${NORMAL}"
    echo -e "${MENU}${NUMBER} 6) ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.(PIG)(MR)(HIVE)${NORMAL}"
    echo -e "${MENU}${NUMBER} 7) ${MENU} Create a bar graph to depict the number of applications for each year(MR)(PIG)(HIVE)${NORMAL}"
    echo -e "${MENU}${NUMBER} 8) ${MENU}Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order(MR)(HIVE)(PIG)${NORMAL}"
    echo -e "${MENU}${NUMBER} 9) ${MENU}Which are the employers who have highest success rate in petitions more than 70%and total petions filed more than 1000?(PIG)(MR)(HIVE)${NORMAL}"
    echo -e "${MENU}${NUMBER} 10) ${MENU}Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000? (PIG)(MR)(HIVE)${NORMAL}"
    echo -e "${MENU}${NUMBER} 11) ${MENU}Export result for option no 12 to MySQL database.(SQOOP,MYSQL)${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}



clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1A) clear;
        option_picked "1A) Is the number of petitions with Data Engineer job title increasing over time?";
                 hadoop fs -rm -r /H1B_Project/MapReduce/Output/Q1ADataEngg
		 hadoop jar h1bproject.jar Q1A_DataEngineerJob /user/hive/warehouse/h1bproject.db/h1b_final /H1B_Project/MapReduce/Output/Q1ADataEngg
		 hadoop fs -cat /H1B_Project/MapReduce/Output/Q1ADataEngg/p*
        show_menu;
        ;;

	1B) clear;
        option_picked "1B) Find top 5 job titles who are having highest growth in applications. ";
		
               pig -x local /home/hduser/Desktop/H1B-VisaProject/Pig/Q1B_Top5JobTitlesAvgGrowthYear.pig;
               
        show_menu;
        ;;

	2A) clear;
        option_picked "2A) Which part of the US has the most Data Engineer jobs for each year?";
	 echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
     
	    hive -e "select worksite, case_status,count(*) as jobs from h1bproject.h1b_final where job_title like '%DATA ENGINEER%' and case_status='CERTIFIED' and year='$var' group by worksite,year,case_status order by jobs desc limit 1;" 
        show_menu;	
        ;;

	2B) clear;
        option_picked "2B) Find top 5 locations in the US who have got certified visa for each year.";
        echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
	    hive -e "select year, worksite,count(*) as status from h1bproject.h1b_final where case_status='CERTIFIED' and year='$var' group by worksite,year order by status desc limit 5;" 
        show_menu;
        ;;

	3) clear;
        option_picked "3) Which industry has the most number of Data Scientist positions?";
	 
         hive -e  "select soc_name, count(*) as dscount from h1bproject.h1b_final where job_title like '%DATA SCIENTIST%' and case_status='CERTIFIED' group by soc_name order by dscount desc limit 1;"

        show_menu;
        ;;

	4) clear;
        option_picked "4)Which top 5 employers file the most petitions each year?";
                 echo -e "Enter the year (2011,2012,2013,2014,2015,2016,ALL)"
                    read var
                  hadoop fs -rm -r  /H1B_Project/MapReduce/Output/Q4Top5Petitions/output
		 hadoop jar h1bproject.jar Q4_Top5Employers  /user/hive/warehouse/h1bproject.db/h1b_final  /H1B_Project/MapReduce/Output/Q4Top5Petitions/output $var;
		 hadoop fs -cat /H1B_Project/MapReduce/Output/Q4Top5Petitions/output/p*	
        show_menu;
        ;;
           
	5A) clear;
        option_picked "5A) Find the most popular top 10 job positions for H1B visa applications for each year?";
	    echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
	    hive -e "select job_title, year,count(*) as total  from h1bproject.h1b_final where year = '$var' group by job_title,year order by total desc limit 10;"
        show_menu;
        ;;

        5B) clear;
        option_picked "5B)Find the most popular top 10 job positions for Certified H1B visa applications for each year ?";
	    echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
	    hive -e "select job_title, year,count(*) as total  from h1bproject.h1b_final where year = '$var' and case_status = 'CERTIFIED' group by job_title,year order by total desc limit 10;"
        show_menu;
        ;;
       
	6) clear;
       option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.";
          
               echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
               echo -e "No of Total Applications For ${var}"
             pig -x local -p whichyear=${var} -f /home/hduser/Desktop/H1B-VisaProject/Pig/Q6_CountOfTotalApplications.pig;
		
        show_menu;
        ;;  

	7) clear;
		
        option_picked "7) Create a bar graph to depict the number of applications for each year";
	         echo -e "Enter the year (2011,2012,2013,2014,2015,2016,ALL)"
                  read var

            hadoop fs -rm -r /H1B_Project/MapReduce/Output/Q7NoOfApplication/output
              hadoop jar h1bproject.jar Q7_NoOfApplicationEachYear  /user/hive/warehouse/h1bproject.db/h1b_final /H1B_Project/MapReduce/Output/Q7NoOfApplication/output $var;
               hadoop fs -cat /H1B_Project/MapReduce/Output/Q7NoOfApplication/output/p*	
                    
		
        show_menu;
        ;;           
        
	8) clear;
        option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order";
		
		echo -e "Enter the case_status (1-Full time position,2-Part time position)";
			read n
			case "$n" in 
			"1")
                         option_picked "Full Time Position" 
                         echo -e "Enter the year (2011,2012,2013,2014,2015,2016,ALL)"
                          read var
			hadoop fs -rm -r  /H1B_Project/MapReduce/Output/Q8AAvgPrWageForEachJobForEachYearFullTime/output
		 hadoop jar h1bproject.jar Q8A_AvgPrWageForEachJobForEachYearFullTime  /user/hive/warehouse/h1bproject.db/h1b_final  /H1B_Project/MapReduce/Output/Q8AAvgPrWageForEachJobForEachYearFullTime/output $var;
		 hadoop fs -cat /H1B_Project/MapReduce/Output/Q8AAvgPrWageForEachJobForEachYearFullTime/output/p*
        ;;

			"2")
			  option_picked "Part Time Position" 
                         echo -e "Enter the year (2011,2012,2013,2014,2015,2016,ALL)"
                          read var
                         hadoop fs -rm -r  /H1B_Project/MapReduce/Output/Q8BAvgPrWageForEachJobForEachYearPartTime/Output
                        hadoop jar h1bproject.jar Q8B_AvgPrWageForEachJobForEachYearPartTime  /user/hive/warehouse/h1bproject.db/h1b_final /H1B_Project/MapReduce/Output/Q8BAvgPrWageForEachJobForEachYearPartTime/Output $var;
                      hadoop fs -cat /H1B_Project/MapReduce/Output/Q8BAvgPrWageForEachJobForEachYearPartTime/Output/p*
        ;;

		esac
		show_menu;
		;;

	9) clear;
		option_picked "9) Which are the employers who have highest success rate in petitions more than 70%and total petions filed more than 1000?"
		pig -x local /home/hduser/Desktop/H1B-VisaProject/Pig/Q9_EmployersSuccessRate.pig;
        show_menu;
        ;;

	
	10) clear;
		option_picked "10) Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000?"
		pig /home/hduser/Desktop/H1B-VisaProject/Pig/Q10_JobPositionsSucessRate.pig;
        show_menu;
        ;;
       11) clear;
		option_picked "11) Export Result Of option no 10 to MySQL Database."

                bash /home/hduser/Desktop/H1B-VisaProject/Sqoop/Q11_ExportQ10AnsToMySqlDBUsingSqoop.sh
                
        show_menu;
        ;;
         
	
        \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done

