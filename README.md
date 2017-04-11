# vrops-export
Simple utility for exporting data from vRealize Operations.

## Description
A simple command-line data export tool for vRealize Operations. Currently supports CVS, but additional output formats are planned.

## Installing the binaries
### Prerequisites
* Java JDK 1.8 installed on the machine where you plan to run the tool
* vRealize Operations 6.3 or higher
### Installation on Linux, Mac or other UNIX-like OS
1. Download the binaries from here: https://www.dropbox.com/s/a4t9cl3qxpdj9gk/vrops-export-1.0-SNAPSHOT-bin.zip?dl=0
2. Unzip the files:
```
mkdir ~/vrops-export
cd ~/vrops-export
unzip vrops-export-<version>-bin.zip
cd vrops-export-<version>/bin
```
3. Make the start script runnable
```chmod +x exporttool.sh```
4. Run it!
```
./exporttool.sh -d samples/vmfields.yaml -u admin -p password -H https://my.vrops.host -i
```

# Building from source
### Prerequisites
* Java JDK 1.8 installed on the machine where you plan to run the tool
* vRealize Operations 6.3 or higher
* Maven 3.3.3 or higher

### Build
1. Get from git
```
git init # Only needed of you haven't already initialized a git repo in the directory 
git clone https://github.com/prydin/vrops-export.git
```
2. Build the code
```
cd vrops-export
mvn package
```
3. Run it!
```
cd target
chmod +x exporttool.sh
./exporttool.sh -d ../samples/vmfields.yaml -u admin -p password -H https://my.vrops.host -i

## Command syntax
```
usage: exporttool [-d <arg>] [-H <arg>] [-h] [-i] [-l <arg>] [-n <arg>]
       [-o <arg>] [-p <arg>] [-q] [-u <arg>]
Exports vRealize Operations Metrics
 -d,--definition <arg>   Path to definition file
 -H,--host <arg>         URL to vRealize Operations Host
 -h,--help               Print a short help
 -i,--ignore-cert        Trust any cert
 -l,--lookback <arg>     Lookback time
 -n,--namequery <arg>    Name query
 -o,--output <arg>       Output file
 -p,--password <arg>     Password
 -q,--quiet              Quiet mode (no progress counter)
 -u,--username <arg>     Username
 ```


