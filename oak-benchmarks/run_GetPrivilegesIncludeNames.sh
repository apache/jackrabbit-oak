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
TITLE=GetPrivilegeCollectionIncludeNamesTest_ACCESSCONTORL_MANAGER_HAS_PRIVILEGES
BENCH="GetPrivilegeCollectionIncludeNamesTest"
ITEMS_TO_READ="10000"
ACE_CNT="1 5 10"
GROUP_CNT="1 10 100"
RUNTIME=5
FIXS="Oak-Segment-Tar"
THREADS="1,10,20" #,50" #"1,2,4,8,10,15,20,50"
PROFILE=true
EVAL_TYPE=ACCESSCONTORL_MANAGER_HAS_PRIVILEGES #ACCESSCONTORL_MANAGER_GET_PRIVILEGE_COLLECTION, PRIVILEGE_MANAGER_INCLUDES, JCR_PRIVILEGE_NAME_AGGREGATION, ACCESSCONTORL_MANAGER_HAS_PRIVILEGES
REPORT=false

LOG=$TITLE"_$(date +'%Y%m%d_%H%M%S').csv"
echo "Benchmarks: $BENCH" > $LOG
echo "Fixture: $FIXS" >> $LOG
echo "Runtime: $RUNTIME" >> $LOG
echo "Concurrency: $THREADS" >> $LOG
echo "Profiling: $PROFILE" >> $LOG
echo "EvalType: $EVAL_TYPE" >> $LOG

echo "Items to Read: $ITEMS_TO_READ" >> $LOG
echo "Number of ACEs per Group Principal: $ACE_CNT" >> $LOG
echo "Number of Groups: $GROUP_CNT" >> $LOG

echo "--------------------------------------" >> $LOG

for bm in $BENCH
    do
    for aceCnt in $ACE_CNT
        do
        for groupCnt in $GROUP_CNT
        do  
            echo "EvalType = $EVAL_TYPE with $aceCnt initial ACEs per principal and $groupCnt principals per subject" | tee -a $LOG
            echo "-----------------------------------------------------------" | tee -a $LOG
            rm -rf target/Jackrabbit-* target/Oak-Tar-*
            cmd="java -Xmx2048m -Dprofile=$PROFILE -Druntime=$RUNTIME -Dwarmup=1 -jar target/oak-benchmarks-*-SNAPSHOT.jar benchmark --itemsToRead $ITEMS_TO_READ --numberOfInitialAce $aceCnt --numberOfGroups $groupCnt --csvFile $LOG --concurrency $THREADS --report $REPORT --evalType $EVAL_TYPE $bm $FIXS"
            echo $cmd
            $cmd
        done
    done
done
echo "-----------------------------------------"
echo "Benchmark completed. see $LOG for details:"
cat $LOG
