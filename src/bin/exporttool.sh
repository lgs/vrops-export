#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VERSION=${project.version}
if [ -z "$JAVA_HOME" ] 
then
	JAVA=java
else
	JAVA="$JAVA_HOME/bin/java"
fi
CP=$DIR/vrops-export-$VERSION.jar
if [ -n "$JDBC_JAR" ]
then
	CP=$CP:$JDBC_JAR
fi
$JAVA -cp $CP -Djsse.enableSNIExtension=false net.virtualviking.vropsexport.Main "$@"