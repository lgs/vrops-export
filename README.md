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
```
chmod +x exporttool.sh
```
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
```

## Command syntax
```
usage: exporttool [-d <arg>] [-H <arg>] [-h] [-i] [-l <arg>] [-n <arg>]
       [-o <arg>] [-p <arg>] [-q] [-u <arg>]
       
 -d,--definition <arg>    Path to definition file
 -e,--end <arg>           Time period end (date format in definition file)
 -F,--list-fields <arg>   Print name and keys of all fields to stdout
 -H,--host <arg>          URL to vRealize Operations Host
 -h,--help                Print a short help
 -i,--ignore-cert         Trust any cert
 -l,--lookback <arg>      Lookback time
 -n,--namequery <arg>     Name query
 -o,--output <arg>        Output file
 -P,--parent <arg>        Parent resource (ResourceKind:resourceName)
 -p,--password <arg>      Password
 -q,--quiet               Quiet mode (no progress counter)
 -s,--start <arg>         Time period start (date format in definition file)
 -u,--username <arg>      Username

 ```
 
 ## Definition file
 The details on what fields to export and how to treat them is expressed in the definition file. This file follows the YAML format. 
 Here is an example of a definition file:
 
 ```
resourceType: VirtualMachine                     # The resource type we're exporting
rollupType: AVG                                  # Rollup type: AVG, MAX, MIN, SUM, LATEST, COUNT
rollupMinutes: 5                                 # Time scale granularity in minutes
dateFormat: yyyy-MM-dd HH:mm:ss                  # Date format. See http://tinyurl.com/pscdf9g
fields:                                          # A list of fields
# CPU fields
  - alias: cpuDemand                             # Name of the field in the output
    metric: cpu|demandPct                        # Reference to a metric field in vR Ops
  - alias: cpuReady
    metric: cpu|readyPct
  - alias: cpuCostop
    metric: cpu|costopPct
# Memory fields
  - alias: memDemand
    metric: mem|object.demand
  - alias: memSwapOut
    metric: mem|swapoutRate_average
  - alias: memSwapIn
    metric: mem|swapinRate_average
 # Storage fields
  - alias: storageDemandKbps
    metric: storage|demandKBps
 # Network fields
  - alias: netBytesRx
    metric: net|bytesRx_average
  - alias: netBytesTx
    metric: net|bytesTx_average
 # Host CPU
  - alias: hostCPUDemand
    metric: $parent:HostSystem.cpu|demandmhz	# Reference to a metric in the parent. 
 # Guest OS
  - alias: guestOS
    prop: config|guestFullName			# Reference to a property (as opposed to metric)
 # Host CPU type
  - alias: hostCPUType
    prop: $parent:HostSystem.cpu|cpuModel		# Reference to a metric in a parent
```
    
## Known issues
Very long time ranges in combination with small interval sizes can cause the server to prematurely close the connection, resulting in NoHttpResponseExceptions to be thrown.
 If this happens, consider shortening the time range.



