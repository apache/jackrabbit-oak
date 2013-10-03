#!/bin/sh
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
