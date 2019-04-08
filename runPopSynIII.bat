
REM # Batch file to run Toronto PopSynIII
REM # Binny M Paul, paulbm@pbworld.com, 2015 Feb
REM ###########################################################################

SET MYSQLSERVER=localhost
SET DATABASE=GTAModelPopSyn
SET DB_USER=root
SET DB_PWD=TashaRuns2019
SET MY_PATH=%CD%
SET MYSQL_EXE="C:\Program Files\MySQL\MySQL Server 5.5\bin\mysql"

SET pumsHH_File='C:\\Users\\brendan\\Documents\\popsyn3-2016\\input\\households_processed.csv'
SET pumsPersons_File='C:\\Users\\brendan\\Documents\\popsyn3-2016\\input\\persons_processed.csv'
SET mazData_File='C:\\Users\\brendan\\Documents\\popsyn3-2016\\input\\gtamodel_maz.csv'
SET tazData_File='C:\\Users\\brendan\\Documents\\popsyn3-2016\\input\\gtamodel_taz.csv'
SET metaData_File='C:\\Users\\brendan\\Documents\\popsyn3-2016\\input\\gta_meta.csv'

REM ###########################################################################

@ECHO OFF
ECHO Toronto PopSynIII

ECHO Creating input tables...
MKDIR outputs
ECHO Creating PUMS Table...
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "source %MY_PATH%\scripts\PUMFTableCreation.sql" > "%MY_PATH%\outputs\serverLog"


ECHO Uploading PUMS Data...
REM ******** Upload Data From CSV Files **********
REM ----------------------------------------------
REM ***Set sql_mode to '' to read empty values as zeros in INT fields**
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "SET sql_mode=''; LOAD DATA INFILE %pumsHH_File% INTO TABLE pumf_hh FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;" > "%MY_PATH%\outputs\serverLog"
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "SET sql_mode=''; LOAD DATA INFILE %pumsPersons_File% INTO TABLE pumf_person FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;" > "%MY_PATH%\outputs\serverLog"

ECHO Processing PUMS tables...
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "source %MY_PATH%\scripts\PUMFTableProcessing.sql" > "%MY_PATH%\outputs\serverLog"

ECHO Creating Control Tables..
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "source %MY_PATH%\scripts\ControlsTableCreation.sql" > "%MY_PATH%\outputs\serverLog"
 
ECHO Uploading Control Tables...
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "SET sql_mode=''; LOAD DATA INFILE %mazData_File% INTO TABLE control_totals_maz FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;" > "%MY_PATH%\outputs\serverLog"
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "SET sql_mode=''; LOAD DATA INFILE %tazData_File% INTO TABLE control_totals_taz FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;" > "%MY_PATH%\outputs\serverLog"
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "SET sql_mode=''; LOAD DATA INFILE %metaData_File% INTO TABLE control_totals_meta FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;" > "%MY_PATH%\outputs\serverLog"

ECHO Processing Control Tables...
CALL %MYSQL_EXE% --host=%SQLSERVER% --user=%DB_USER% --password=%DB_PWD% %DATABASE% -e "source %MY_PATH%\scripts\ControlsTableProcessing.sql" > "%MY_PATH%\outputs\serverLog"


REM ###########################################################################

ECHO %startTime%%Time%
ECHO Running population synthesizer...
SET JAVA_64_PATH="D:\Program Files\Java\jre7"
SET CLASSPATH=runtime\config
SET CLASSPATH=%CLASSPATH%;runtime\*
SET CLASSPATH=%CLASSPATH%;runtime\lib\*
SET CLASSPATH=%CLASSPATH%;runtime\lib\JPFF-3.2.2\JPPF-3.2.2-admin-ui\lib\*
SET LIBPATH=runtime\lib

%JAVA_64_PATH%\bin\java -showversion -server -Xms8000m -Xmx15000m -cp "%CLASSPATH%" -Djppf.config=jppf-clientLocal.properties -Djava.library.path=%LIBPATH% popGenerator.PopGenerator runtime/config/settings.xml
ECHO Population synthesis completed for general population...

