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
TITLE=ManyGroupMembersTest
BENCH="ManyGroupMembersTest"
BATCH_SIZE="1"
IMPORT_BEHAVIORS="besteffort ignore" #abort"
MEMBERS_CNT="1 10 100 1000"
RUNTIME=5
FIXS="Oak-Tar"
THREADS="1,10,20,50" #"1,2,4,8,10,15,20,50"
PROFILE=false

LOG=$TITLE"_$(date +'%Y%m%d_%H%M%S').csv"
echo "Benchmarks: $BENCH" > $LOG
echo "Fixture: $FIXS" >> $LOG
echo "Runtime: $RUNTIME" >> $LOG
echo "Concurrency: $THREADS" >> $LOG
echo "Profiling: $PROFILE" >> $LOG

echo "Batch Size: $BATCH_SIZE" >> $LOG
echo "Import Behavior(s): $IMPORT_BEHAVIORS" >> $LOG
echo "Number of Members: $MEMBERS_CNT" >> $LOG

echo "--------------------------------------" >> $LOG

for bm in $BENCH
    do
    for importBehavior in $IMPORT_BEHAVIORS
        do
        for noMembers in $MEMBERS_CNT
        do
            echo "Executing benchmarks with $noMembers members on $importBehavior" | tee -a $LOG
        echo "-----------------------------------------------------------" | tee -a $LOG
            rm -rf target/Jackrabbit-* target/Oak-Tar-*
            cmd="java -Xmx2048m -Dprofile=$PROFILE -Druntime=$RUNTIME -Dwarmup=1 -jar target/oak-run-*-SNAPSHOT.jar benchmark --batchSize $BATCH_SIZE --importBehavior $importBehavior --numberOfUsers $noMembers --csvFile $LOG --concurrency $THREADS --report false $bm $FIXS"
            echo $cmd
            $cmd
        done
    done
done
echo "-----------------------------------------"
echo "Benchmark completed. see $LOG for details:"
cat $LOG
