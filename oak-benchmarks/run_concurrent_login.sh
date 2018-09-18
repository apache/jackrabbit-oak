#!/bin/sh
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
TITLE=LoginTest
BENCH="LoginWithMembershipTest" #LoginWithMembersTest LoginWithMembershipTest LoginTest LoginLogoutTest LoginGetRootLogoutTest"
USER="user" # admin anonymous"
USE_TOKEN=false # true
HASH_ITERATIONS="-1"
EXPIRATION="200000"
NO_GROUPS="1 10 100 1000"
USE_NESTED_GROUPS=true # false
RUNTIME=5
FIXS="Oak-Segment-Tar" # Jackrabbit"
THREADS="1,10,20,50" #"1,2,4,8,10,15,20,50"
PROFILE=false
NUM_ITEMS=1000

LOG=$TITLE"_$(date +'%Y%m%d_%H%M%S').csv"
echo "Benchmarks: $BENCH" > $LOG
echo "Fixtures: $FIXS" >> $LOG
echo "Runtime: $RUNTIME" >> $LOG
echo "Concurrency: $THREADS" >> $LOG
echo "Profiling: $PROFILE" >> $LOG

echo "User: $USER" >> $LOG
echo "Run with Token: $USE_TOKEN" >> $LOG
echo "Hash Iterations: $HASH_ITERATIONS" >> $LOG
echo "Cache Expiration: $EXPIRATION" >> $LOG
echo "Number of Groups: $NO_GROUPS" >> $LOG
echo "Use Nested Groups: $USE_NESTED_GROUPS" >> $LOG

echo "--------------------------------------" >> $LOG

for bm in $BENCH
    do
    for noGroups in $NO_GROUPS
        do
        # we start new VMs for each fixture to minimize memory impacts between them
        for fix in $FIXS
        do
            echo "Executing benchmarks as user: $USER with $noGroups groups (nested = $USE_NESTED_GROUPS) on $fix" | tee -a $LOG
        echo "-----------------------------------------------------------" | tee -a $LOG
            rm -rf target/Jackrabbit-* target/Oak-Tar-*
            # cmd="java -Xmx2048m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005 -Dprofile=$PROFILE -Druntime=$RUNTIME -Dwarmup=10 -jar target/oak-benchmarks-*-SNAPSHOT.jar benchmark --noIterations $HASH_ITERATIONS --runWithToken $USE_TOKEN --expiration $EXPIRATION --numberOfGroups $noGroups --nestedGroups $USE_NESTED_GROUPS --csvFile $LOG --concurrency $THREADS --runAsUser $USER --report false $bm $fix"
            cmd="java -Xmx2048m -Dprofile=$PROFILE -Druntime=$RUNTIME -Dwarmup=10 -jar target/oak-benchmarks-*-SNAPSHOT.jar benchmark --noIterations $HASH_ITERATIONS --runWithToken $USE_TOKEN --expiration $EXPIRATION --numberOfGroups $noGroups --nestedGroups $USE_NESTED_GROUPS --csvFile $LOG --concurrency $THREADS --runAsUser $USER --report false $bm $fix"
            echo $cmd
            $cmd
        done
    done
done
echo "-----------------------------------------"
echo "Benchmark completed. see $LOG for details:"
cat $LOG
