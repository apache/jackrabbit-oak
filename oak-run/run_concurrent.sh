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
USERS="false true"
RUNTIME=20
#BENCH=ConcurrentReadAccessControlledTreeTest
BENCH=ManyUserReadTest
RANDOM_USER="true"
FIXS="Oak-Tar" # Jackrabbit"
THREADS="0 1 2 4 8 12 16 20"
PROFILE=false

LOG=$BENCH"_$(date +'%Y%m%d_%H%M%S').csv"
echo "Benchmarks: $BENCH" > $LOG
echo "Fixtures: $FIXS" >> $LOG
echo "Users: $USERS" >> $LOG
echo "Runtime: $RUNTIME" >> $LOG
echo "Concurrency: $THREADS" >> $LOG
echo "Random User: $RANDOM_USER" >> $LOG
echo "Profiling: $PROFILE" >> $LOG
echo "--------------------------------------" >> $LOG
for user in $USERS
    do
    echo "Executing benchmarks as admin: $user" | tee -a $LOG
    echo "-----------------------------------------------------------" | tee -a $LOG
    for i in $THREADS
        do
        rm -rf target/Jackrabbit-* target/Oak-Tar-*
        cmd="java -Xmx2048m -Dprofile=$PROFILE -Druntime=$RUNTIME -jar target/oak-run-*-SNAPSHOT.jar benchmark --csvFile $LOG --bgReaders $i --runAsAdmin $user --report false --randomUser $RANDOM_USER $BENCH $FIXS"
        echo $cmd
        $cmd 
    done
done
echo "-----------------------------------------"
echo "Benchmark completed. see $LOG for details:"
cat $LOG
