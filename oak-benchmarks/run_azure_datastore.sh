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
TITLE=AzureDatastoreTest
BENCH="ReadBinaryPropertiesTest"
ADMIN="false" # true"
RUNTIME=360
RANDOM_USER="true"
FIXS="Oak-Segment-Azure"
PROFILE=false
#DEBUG=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005
DEBUG=""

AZURITE="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"
# Set DataStore connection string using DS_AZURE_CONNECTION_STRING environment variable. If not set, defaults to Azurite default test credentials
DATASTORE_AZURE_CONNECTION=${DS_AZURE_CONNECTION_STRING:-$AZURITE}
# Set SegmentStore connection string using AZURE_CONNECTION_STRING environment variable. If not set, defaults to Azurite default test credentials
AZURE_CONNECTION=${AZURE_CONNECTION_STRING:-$AZURITE}
AZURE_CONTAINER="segment"
AZURE_ROOT_PATH="/aem"
DATASTORE_CLASS="org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore"
BINARIES_THRESHOLD="512"
TREE_MAX_NODES=300
TREE_BINARY_SIZE=100000

LOG=$TITLE"_$(date +'%Y%m%d_%H%M%S').csv"
echo "Benchmarks: $BENCH" > $LOG
echo "Fixtures: $FIXS" >> $LOG
echo "Admin User: $ADMIN" >> $LOG
echo "Runtime: $RUNTIME" >> $LOG
echo "Random User: $RANDOM_USER" >> $LOG
echo "Profiling: $PROFILE" >> $LOG
echo "Tree - Max nodes: $TREE_MAX_NODES" >> $LOG
echo "Tree - binary value size: $TREE_BINARY_SIZE" >> $LOG
echo "--------------------------------------" >> $LOG

rm -rf target/oak-benchmarks-*-tests.jar # remove unused but conflicting jar, if present

# Generate Datastore config file. Escape `=` in connection string
echo "azureConnectionString=\"$DATASTORE_AZURE_CONNECTION\"" | sed -e 's/=/\\=/g' -e 's/\\=/=/1' > target/datastore.properties
# Set DataStore#minRecordLength to same value as BINARIES_THRESHOLD to force writing binary records to the BlobStore instead of keeping them in memory
echo "minRecordLength=\"$BINARIES_THRESHOLD\"" >> target/datastore.properties
# Disable local caching of binary data
echo "cacheSize=\"0\"" >> target/datastore.properties

for bm in $BENCH
    do
    for user in $ADMIN
        do
        # we start new VMs for each fixture to minimize memory impacts between them
        for fix in $FIXS
        do
            echo "Executing benchmarks as admin: $user on $fix" | tee -a $LOG
            echo "-----------------------------------------------------------" | tee -a $LOG
            rm -rf target/$FIXS-* # Remove dangling local repository caches, if any 
            cmd="java $DEBUG -Xmx2048m -Dprofile=$PROFILE -Druntime=$RUNTIME -DskipWarmup=true -DmaxNodes=$TREE_MAX_NODES -DbinaryValueSize=$TREE_BINARY_SIZE -DdataStore=$DATASTORE_CLASS -Dds.config=target/datastore.properties -jar target/oak-benchmarks-*.jar benchmark --csvFile $LOG --runAsAdmin $user --report false --randomUser $RANDOM_USER --binariesInlineThreshold $BINARIES_THRESHOLD --azure $AZURE_CONNECTION --azureContainerName $AZURE_CONTAINER --azureRootPath $AZURE_ROOT_PATH $bm $fix"
            echo $cmd
            $cmd
        done
    done
done
echo "-----------------------------------------"
echo "Benchmark completed. see $LOG for details:"
cat $LOG
