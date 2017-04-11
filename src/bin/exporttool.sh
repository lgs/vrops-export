#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [ -z "$JAVA_HOME" ] 
then
	JAVA=java
else
	JAVA="$JAVA_HOME/bin/java"
fi
$JAVA -Djsse.enableSNIExtension=false -jar $DIR/vrops-export-*.jar $*