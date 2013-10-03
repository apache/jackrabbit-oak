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
AS_ADMIN=false
RUNTIME=20
BENCH=ConcurrentReadAccessControlledTreeTest
for fix in Oak-Tar Jackrabbit 
    do
    for i in 1 2 4 8 10 12 14 16 18 20 
    do
	echo "Executing benchmark with $i threads on $fix"
        rm -rf target/jackrabbit-*
	cmd="java -Xmx2048m -Druntime=$RUNTIME -jar target/oak-run-0.10-SNAPSHOT.jar benchmark --bgReaders $i --runAsAdmin $AS_ADMIN --report false $BENCH $fix"
	echo $cmd
	$cmd
    done
done
