@ECHO OFF
SET DIR=%~dp0
SET VERSION=${project.version}
SET JAVA=java
IF [%JAVA_HOME%] == [] GOTO :NO_JAVA_HOME
SET JAVA="%JAVA_HOME%\bin\java"
:NO_JAVA_HOME
SET CP=%DIR%\vrops-export-%VERSION%.jar
IF [%JDBC_JAR%] == [] GOTO :NO_JDBC_JAR
SET CP=%CP%;%JDBC_JAR%
:NO_JDBC_JAR
JAVA -cp %CP% -Djsse.enableSNIExtension=false net.virtualviking.vropsexport.Main %*