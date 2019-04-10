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
