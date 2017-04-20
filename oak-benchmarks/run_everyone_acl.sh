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
TITLE=ConcurrentEveryoneACLTest
BENCH="ConcurrentEveryoneACLTest"
ADMIN="false"
RUNTIME=5
RANDOM_USER="true"
FIXS="Oak-Segment-Tar" # Jackrabbit"
THREADS="1,2,4,8,10,15,20,50"
PROFILE=false
NUM_ITEMS=1000

LOG=$TITLE"_$(date +'%Y%m%d_%H%M%S').csv"
echo "Benchmarks: $BENCH" > $LOG
echo "Fixtures: $FIXS" >> $LOG
echo "Admin User: $ADMIN" >> $LOG
echo "Runtime: $RUNTIME" >> $LOG
echo "Num Items: $NUM_ITEMS" >> $LOG
echo "Concurrency: $THREADS" >> $LOG
echo "Random User: $RANDOM_USER" >> $LOG
echo "Profiling: $PROFILE" >> $LOG
echo "--------------------------------------" >> $LOG

for bm in $BENCH
    do
    for user in $ADMIN
        do
        # we start new VMs for each fixture to minimize memory impacts between them
        for fix in $FIXS
        do
            echo "Executing benchmarks as admin: $user on $fix" | tee -a $LOG
        echo "-----------------------------------------------------------" | tee -a $LOG
            rm -rf target/Jackrabbit-* target/Oak-Tar-*
            cmd="java -Xmx2048m -Dprofile=$PROFILE -Druntime=$RUNTIME -Dwarmup=5 -jar target/oak-benchmarks-*-SNAPSHOT.jar benchmark --itemsToRead $NUM_ITEMS --csvFile $LOG --concurrency $THREADS --runAsAdmin $user --report false --randomUser $RANDOM_USER $bm $fix"
            echo $cmd
            $cmd
        done
    done
done
echo "-----------------------------------------"
echo "Benchmark completed. see $LOG for details:"
cat $LOG
