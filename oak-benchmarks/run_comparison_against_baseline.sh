#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e

BENCHMARKS="BasicWriteTest LoginTest LoginLogoutTest LoginGetRootLogoutTest NamespaceTest NamespaceRegistryTest ReadPropertyTest GetNodeWithAdmin GetNodeWithAnonymous GetDeepNodeTest SetPropertyTest SetMultiPropertyTest SmallFileReadTest SmallFileWriteTest ConcurrentReadTest ConcurrentReadWriteTest ConcurrentWriteReadTest ConcurrentWriteTest SimpleSearchTest UUIDLookupTest SQL2SearchTest DescendantSearchTest SQL2DescendantSearchTest FlatTreeUpdateTest CreateManyChildNodesTest CreateManyNodesTest UpdateManyChildNodesTest TransientManyChildNodesTest ReadDeepTreeTest ConcurrentReadDeepTreeTest ConcurrentReadRandomNodeAndItsPropertiesTest ConcurrentTraversalTest ConcurrentCreateNodesTest SequentialCreateNodesTest ConcurrentFileWriteTest IsNodeTypeTest SetPropertyTransientTest"
FIXTURES="Oak-Mongo"

RUNTIME=60
THREADS="1"
PROFILE=false
NUM_ITEMS=1000

RESET='\033[0m'
RED='\033[31m'
GREEN='\033[32m'
YELLOW='\033[33m'

me=`basename "$0"`
repositoryurl="https://github.com/apache/jackrabbit-oak/archive/jackrabbit-oak-version.tar.gz"

function usage() {
    cat <<-EOF
THis script runs a set of benchmarks to compare 2 different versions of the application. The base version is the baseline to be compared with,
while the current version is the new version to compare against the baseline.

This script will download and build the sources of the software from the repository archive in case they are not present in the local system:
$repositoryurl
    
USAGE: $me [OPTIONS]

OPTIONS

  -bv   Base version to compare results with.
  -bp   Path to the base version of oak-benchmarks.jar in the local system. If not specified sources will be downloaded and built before execution.
  -bo   Location of an existing benchmarks output log for the base version. If specified this file will be read instead of running the benchmarks for the base version.
  -bs   Path to the base version sources of oak-benchmarks in the local system. If specified, the pom.xml file in this folder will be built using maven instead of downloading the sources.
  -cv   Current version to compare results with baseline.
  -cp   Path to the current version of oak-benchmarks.jar in the local system. If not specified sources will be downloaded and built before execution.
  -co   Location of an existing benchmarks output log for the current version. If specified this file will be read instead of running the benchmarks for the current version.
  -cs   Path to the current version sources of oak-benchmarks in the local system. If specified, the pom.xml file in this folder will be build using maven instead of downloading the sources.
  
  -of   Folder to save all output files generated and downloaded by the script. If not specified, "./target" will be used.
  -nc   Not colorized output.
  -do   Date in output filenames.
  
  --fixtures         String containing all the fixtures to be executed.
  --benchmarks       String containing all the benchmarks to be executed.
  --extraargs        String containing all the additional arguments to be passed to the benchmarks. For example: "--mongouri mongodb://127.0.0.1:27017/test"

  --baseversion      Same as -bv
  --basepath         Same as -bp
  --baseoutput       Same as -bo
  --basesource       Same as -bs
  --currentversion   Same as -cv
  --currentpath      Same as -cp
  --currentoutput    Same as -co
  --currentsource    Same as -cs
  
  --outputfolder     Same as -of
  --nocolor          Same as -nc
  --dateoutput       Same as -do
EOF
}

if [[ $# -lt 1 ]] ; then
    echo "Error: Incorrect number of arguments."
    usage
    exit 1
fi;

# Configuring arguments
while [[ ${1:0:1} == - ]]; do
    [[ $1 =~ ^(-h|--help)$ ]] && { usage; exit 0; };
    [[ $1 =~ ^(-bv|--baseversion)$ ]] && { baseversion=$2; shift 2; continue; };
    [[ $1 =~ ^(-bp|--basepath)$ ]] && { basepath=$2; shift 2; continue; };
    [[ $1 =~ ^(-bo|--baseoutput)$ ]] && { baseoutput=$2; shift 2; continue; };
    [[ $1 =~ ^(-bs|--basesource)$ ]] && { basesource=$2; shift 2; continue; };
    [[ $1 =~ ^(-cv|--currentversion)$ ]] && { currentversion=$2; shift 2; continue; };
    [[ $1 =~ ^(-cp|--currentpath)$ ]] && { currentpath=$2; shift 2; continue; };
    [[ $1 =~ ^(-co|--currentoutput)$ ]] && { currentoutput=$2; shift 2; continue; };
    [[ $1 =~ ^(-cs|--currentsource)$ ]] && { currentsource=$2; shift 2; continue; };
    
    [[ $1 =~ ^(-of|--outputfolder)$ ]] && { outputfolder=$2; shift 2; continue; };
    [[ $1 =~ ^(-nc|--nocolor)$ ]] && { nocolor=1; shift 1; continue; };
    [[ $1 =~ ^(-do|--dateoutput)$ ]] && { dateoutput=1; shift 1; continue; };
    
    [[ $1 =~ ^(--fixtures)$ ]] && { fixtures=$2; shift 2; continue; };
    [[ $1 =~ ^(--benchmarks)$ ]] && { benchmarks=$2; shift 2; continue; };
    [[ $1 =~ ^(--extraargs)$ ]] && { extraargs=$2; shift 2; continue; };
    
    echo "Error: Unknown parameter found: $1"
    exit 1;
done

if [[ $nocolor -eq 1 ]]; then
    RESET=''
    RED=''
    GREEN=''
    YELLOW=''
fi;

if [[ -z "$outputfolder" ]]; then
    outputfolder="./target"
fi
mkdir -p "$outputfolder"

if [[ $dateoutput -eq 1 ]]; then
    LOG_DATE="$(date +'%Y%m%d_%H%M%S')"
    LOG_BASE="$outputfolder/baseVersionOutput_$LOG_DATE.csv"
    LOG_CURRENT="$outputfolder/currentVersionOutput_$LOG_DATE.csv"
    LOG_RESULT="$outputfolder/comparisonResult_$LOG_DATE.csv"
    LOG_COMPARISON="$outputfolder/comparisonReport_$LOG_DATE.csv"
else
    LOG_BASE="$outputfolder/baseVersionOutput.csv"
    LOG_CURRENT="$outputfolder/currentVersionOutput.csv"
    LOG_RESULT="$outputfolder/comparisonResult.csv"
    LOG_COMPARISON="$outputfolder/comparisonReport.csv"
fi;

# Check if both versions are set (required arguments)
if [[ -z $baseversion ]]; then 
    echo "Error: Base version not specified."
    exit 1
fi;
if [[ -z $currentversion ]]; then
    echo "Error: Current version not specified."
    exit 1
fi;

# Check if baseout is set, otherwise the benchmarks should be executed for base version
if [[ -n $baseoutput ]]; then
    echo "Reading output file $baseoutput for base version."
    
    if [[ ! -f $baseoutput ]]; then
        echo "Error: Output file $baseoutput cannot be read."
        exit 1;
    fi;
else
    basemvnpath="$outputfolder/jackrabbit-oak-jackrabbit-oak-$baseversion/oak-benchmarks"
    if [[ -z $basepath ]]; then
        if [[ -n $basesource ]]; then
            echo "Using sources in $basesource for base oak-benchmarks"
            basemvnpath="$basesource"
        else
            echo "Path to base version not specified. Downloading sources... ${repositoryurl/version/$baseversion}"
            wget -O "$outputfolder/jackrabbit-oak-$baseversion.tar.gz" "${repositoryurl/version/$baseversion}" -nv
            if [[ $? -ne 0 ]]; then
                echo "Error: Base version couldn't be downloaded."
                exit 1
            fi;
            
            echo "Unpacking sources..."
            tar xf "$$outputfolder/jackrabbit-oak-$baseversion.tar.gz" -C "$outputfolder"
            if [[ $? -ne 0 ]]; then
                echo "Error: Sources couldn't be unpacked."
                exit 1
            fi;
        fi;
        
        echo "Building oak-benchmarks in $basemvnpath"
        mvn -B -f "$basemvnpath" install
        if [[ $? -ne 0 ]]; then
            echo "Error: Maven failed building the project."
            exit 1
        fi;
        basepath="$basemvnpath/target/oak-benchmarks-$baseversion.jar"
    fi;

    if [[ ! -f "./$basepath" ]]; then
        echo "Error: Path to base version $basepath is not found."
        exit 1
    fi;
fi;

# Check if baseout is set, otherwise the benchmarks should be executed for current version
if [[ -n $currentoutput ]]; then
    echo "Reading output file $currentoutput for current version."
    
    if [[ ! -f $currentoutput ]]; then
        echo "Error: Output file $currentoutput cannot be read."
        exit 1;
    fi;
else
    currentmvnpath="jackrabbit-oak-jackrabbit-oak-$currentversion/oak-benchmarks"
    if [[ -z $currentpath ]]; then
        if [[ -n $currentsource ]]; then
            echo "Using sources in $currentsource for current oak-benchmarks"
            currentmvnpath="$currentsource"
        else
            echo "Path to current version not specified. Downloading sources... ${repositoryurl/version/$currentversion}"
            wget -O "jackrabbit-oak-$currentversion.tar.gz" "${repositoryurl/version/$currentversion}" -nv
            if [[ $? -ne 0 ]]; then
                echo "Error: Current version couldn't be downloaded."
                exit 1
            fi;
            
            echo "Unpacking sources..."
            tar xf "jackrabbit-oak-$currentversion.tar.gz"
            if [[ $? -ne 0 ]]; then
                echo "Error: Sources couldn't be unpacked."
                exit 1
            fi;
        fi;
        
        echo "Building oak-benchmarks in $currentmvnpath"
        mvn -B -f "$currentmvnpath" install
        if [[ $? -ne 0 ]]; then
            echo "Error: Maven failed building the project."
            exit 1
        fi;
        currentpath="$currentmvnpath/target/oak-benchmarks-$currentversion.jar"
    fi;

    if [[ ! -f "./$currentpath" ]]; then
        echo "Error: Path to current version $currentpath is not found."
        exit 1
    fi;
fi;

if [[ -z $benchmarks ]]; then
    benchmarks="$BENCHMARKS"
fi;
if [[ -z $fixtures ]]; then
    fixtures="$FIXTURES"
fi;

# Running benchmarks for base version in case $baseoutput is not set
if [[ -z $baseoutput ]]; then
    echo "-----------------------------------------------------------"
    echo "Executing benchmarks for version $baseversion"
    echo "-----------------------------------------------------------"
    baseoutput="$LOG_BASE"
    for benchmark in $benchmarks
    do
        for fixture in $fixtures
        do
            rm -rf "jackrabbit-oak-jackrabbit-oak-$baseversion/oak-benchmarks/target/Jackrabbit-*" "jackrabbit-oak-jackrabbit-oak-$baseversion/oak-benchmarks/target/Oak-Tar-*"
            cmd="java -Xmx2048m -jar $basepath benchmark --csvFile $LOG_BASE --cache 256 $extraargs $benchmark $fixture"
            echo $cmd
            $cmd
        done
    done
    echo "-----------------------------------------"
    echo "Benchmark completed. see $LOG_BASE for details:"
    cat $LOG_BASE
fi;

# Running benchmarks for current version in case $currentoutput is not set
if [[ -z $currentoutput ]]; then
    echo "-----------------------------------------------------------"
    echo "Executing benchmarks for version $currentversion"
    echo "-----------------------------------------------------------"
    currentoutput="$LOG_CURRENT"
    for benchmark in $benchmarks
    do
        for fixture in $fixtures
        do
            rm -rf "jackrabbit-oak-jackrabbit-oak-$currentversion/oak-benchmarks/target/Jackrabbit-*" "jackrabbit-oak-jackrabbit-oak-$currentversion/oak-benchmarks/target/Oak-Tar-*"
            cmd="java -Xmx2048m -jar $currentpath benchmark --csvFile $LOG_CURRENT --cache 256 $extraargs $benchmark $fixture"
            echo $cmd
            $cmd
        done
    done
    echo "-----------------------------------------------------------"
    echo "Benchmarks completed. see $LOG_CURRENT for details:"
    cat $LOG_CURRENT
fi;

IFS=,

echo ''
echo "-----------------------------------------------------------"
echo "Comparing results of $currentversion against $baseversion"
echo "-----------------------------------------------------------"
while read test c min r10 r50 r90 max n; do
    # Skip line if starts with #
    if [[ ${test:0:1} == "#" ]]; then
        Btest+=(${test:2})
        continue
    fi;
    Bc+=($c)
    Bmin+=($min)
    B10+=($r10)
    B50+=($r50)
    B90+=("${r90// /}") # Remove all spaces
    Bmax+=($max)
    Bn+=($n)
done < $baseoutput

while read test c min r10 r50 r90 max n; do
    # Skip line if starts with #
    if [[ ${test:0:1} == "#" ]]; then 
        Ctest+=(${test:2})
        continue
    fi;
    Cc+=($c)
    Cmin+=($min)
    C10+=($r10)
    C50+=($r50)
    C90+=("${r90// /}") # Remove all spaces
    Cmax+=($max)
    Cn+=($n)
done < $currentoutput

if [[ ${#Btest[@]} -ge ${#Ctest[@]} ]]; then
    elements=${#Ctest[@]}
else
    elements=${#Btest[@]}
fi;

# Loop through results calculating the difference ratio
total=0
max=0
min=1000
skipped=0

echo ''
echo 'Average time to run Benchmarks for current and base versions. Lower value is better:'
echo '----------------------------------------------------------------------------------------------'
printf "| %-30s | %-14s | %-14s | %-10s | %-10s |\n" 'TEST NAME' 'BASE (ms)' 'CURRENT (ms)' 'RATIO' 'PERCENTAGE'
printf "%-30s, %-14s, %-14s, %-10s, %-10s\n" 'TEST NAME' 'BASE (ms)' 'CURRENT (ms)' 'RATIO' 'PERCENTAGE' >> $LOG_COMPARISON
echo '----------------------------------------------------------------------------------------------'
for ((i=0;i<=elements-1;i++)); do
    basetest=${Btest[$i]}
    currenttest=${Ctest[$i]}
    
    if [[ "$basetest" != "$currenttest" ]]; then
        skipped=$skipped+1
        continue
    fi;
    
    basevalue=${B90[$i]}
    currentvalue=${C90[$i]}
    ratio=$(echo "scale=4 ; $currentvalue / $basevalue" | bc)
    if (( $(echo "$ratio > $max" |bc -l) )); then
        max=$ratio
    fi;
    if (( $(echo "$ratio < $min" |bc -l) )); then
        min=$ratio
    fi;
    total=$(echo "scale=4 ; $total + $ratio" | bc)
    percentage=$(echo "scale=2 ; ($ratio * 100 - 100) /1" | bc)
    if (( $(echo "$percentage > 0" |bc) )); then
        printf "${RESET}| %-30s | %-14s | %-14s | ${RED}%-10s ${RESET}| ${RED}%-10s ${RESET}| \n" $basetest $basevalue $currentvalue $ratio $percentage
    elif (( $(echo "$percentage < 0" |bc) )); then
        printf "${RESET}| %-30s | %-14s | %-14s | ${GREEN}%-10s ${RESET}| ${GREEN}%-10s ${RESET}| \n" $basetest $basevalue $currentvalue $ratio $percentage
    else
        printf "${RESET}| %-30s | %-14s | %-14s | %-10s ${RESET}| %-10s ${RESET}| \n" $basetest $basevalue $currentvalue $ratio $percentage
    fi;
    printf '%-30s, %-14s, %-14s, %-10s, %-10s\n' $basetest $basevalue $currentvalue $ratio $percentage >> $LOG_COMPARISON
done
echo '----------------------------------------------------------------------------------------------'

if [[ $skipped -gt 0 ]]; then
    echo -e "${YELLOW}Skipped $skipped tests because order doesn't match${RESET}\n"
fi;

echo ""
echo "Performance difference between versions:"
average=$(echo "scale=4 ; $total / $elements" | bc)

# Show the average, maximum and minimum ratio
echo '----------------------------------------------------'
printf "| %-14s | %-14s | %-14s |\n" 'AVERAGE' 'MAXIMUM' 'MINIMUM'
echo '----------------------------------------------------'
printf "| %-14s | %-14s | %-14s |\n" $average $max $min
echo '----------------------------------------------------'

printf "%-14s, %-14s, %-14s\n" 'AVERAGE' 'MAXIMUM' 'MINIMUM' >> $LOG_RESULT
printf "%-14s, %-14s, %-14s\n" $average $max $min >> $LOG_RESULT

echo "Results have been saved to file $LOG_RESULT"
