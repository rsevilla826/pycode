# pycode
This is a random collection of my python work with databases. I will upload when I can or when I have time.

SnowConnections.py - Is a python class that reads in a JSON file to get connection info and connect to Snowflake. 

sf_util.py - This script will either execute all dot run_ sql files in a directory or
             take -f <sql_file_name>, as an argument to execute a single sql script. If you would like
             to cycle through and execute all files in the SQL directory, pass option -d. If you pass -f 
             <sql_file_name>. This script can also dump a Snowflake table into a CSV file and compress that file.
