/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import org.junit.Ignore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.junit.Assert.assertNotNull;

public class BlobThroughPutTest {
    private static final int NO_OF_NODES = 100;
    private static final int BLOB_SIZE = 1024 * 1024 * 2;

    private static final String TEST_DB1 = "tptest1";
    private static final String TEST_DB2 = "tptest2";

    private static final int MAX_EXEC_TIME = 5; //In seconds
    private static final int[] READERS = {5, 10, 15, 20};
    private static final int[] WRITERS = {0, 1, 2, 4};

    private final List<Result> results = new ArrayList<Result>();

    private static final BiMap<WriteConcern,String> namedConcerns;

    static {
        BiMap<WriteConcern,String> bimap = HashBiMap.create();
        bimap.put(WriteConcern.FSYNC_SAFE,"FSYNC_SAFE");
        bimap.put(WriteConcern.JOURNAL_SAFE,"JOURNAL_SAFE");
//        bimap.put(WriteConcern.MAJORITY,"MAJORITY");
        bimap.put(WriteConcern.UNACKNOWLEDGED,"UNACKNOWLEDGED");
        bimap.put(WriteConcern.NORMAL,"NORMAL");
//        bimap.put(WriteConcern.REPLICAS_SAFE,"REPLICAS_SAFE");
        bimap.put(WriteConcern.SAFE,"SAFE");
        namedConcerns = Maps.unmodifiableBiMap(bimap);
    }

    private final String localServer = "localhost:27017/test";
    private final String remoteServer = "remote:27017/test";

    @Ignore
    @Test
    public void performBenchMark() throws InterruptedException {
        MongoClient local = new MongoClient(new MongoClientURI(localServer));
        MongoClient remote = new MongoClient(new MongoClientURI(remoteServer));

        run(local, false, false);
        run(local, true, false);
        run(remote, false, true);
        run(remote, true, true);

        dumpResult();
    }

    @Ignore
    @Test
    public void performBenchMark_WriteConcern() throws InterruptedException {
        MongoClient mongo = new MongoClient(new MongoClientURI(remoteServer));
        final MongoDatabase db = mongo.getDatabase(TEST_DB1);
        final MongoCollection<BasicDBObject> nodes = db.getCollection("nodes", BasicDBObject.class);
        final MongoCollection<BasicDBObject> blobs = db.getCollection("blobs", BasicDBObject.class);
        int readers = 0;
        int writers = 2;
        for(WriteConcern wc : namedConcerns.keySet()){
            prepareDB(nodes, blobs);
            final Benchmark b = new Benchmark(nodes, blobs);
            Result r = b.run(readers, writers, true, wc);
            results.add(r);
        }

        prepareDB(nodes, blobs);

        dumpResult();
    }

    private void dumpResult() {
        PrintStream ps = System.out;

        ps.println(Result.OUTPUT_FORMAT);

        for (Result r : results) {
            ps.println(r.toString());
        }
    }

    private void run(MongoClient mongo, boolean useSameDB, boolean remote) throws InterruptedException {
        final MongoDatabase nodeDB = mongo.getDatabase(TEST_DB1);
        final MongoDatabase blobDB = useSameDB ? mongo.getDatabase(TEST_DB1) : mongo.getDatabase(TEST_DB2);
        final MongoCollection<BasicDBObject> nodes = nodeDB.getCollection("nodes", BasicDBObject.class);
        final MongoCollection<BasicDBObject> blobs = blobDB.getCollection("blobs", BasicDBObject.class);

        for (int readers : READERS) {
            for (int writers : WRITERS) {
                prepareDB(nodes, blobs);
                final Benchmark b = new Benchmark(nodes, blobs);
                Result r = b.run(readers, writers, remote, WriteConcern.SAFE);
                results.add(r);
            }
        }
    }

    private void prepareDB(MongoCollection<BasicDBObject> nodes,
                           MongoCollection<BasicDBObject> blobs) {
        MongoUtils.dropCollections(nodes.getNamespace().getDatabaseName());
        MongoUtils.dropCollections(blobs.getNamespace().getDatabaseName());

        createTestNodes(nodes);
    }

    private void createTestNodes(MongoCollection<BasicDBObject> nodes) {
        for (int i = 0; i < NO_OF_NODES; i++) {
            BasicDBObject obj = new BasicDBObject("_id", i)
                    .append("foo", "bar1" + i);
            nodes.insertOne(obj);
        }
    }

    private static class Result {
        final static String OUTPUT_FORMAT = "remote, samedb, readers, writers, reads, writes, " +
                "time, readThroughPut, writeThroughPut, writeConcern";
        int totalReads;
        int totalWrites = 0;
        int noOfReaders;
        int noOfWriters;
        int execTime;
        int dataSize = BLOB_SIZE;
        boolean sameDB;
        boolean remote;
        WriteConcern writeConcern;

        double readThroughPut() {
            return totalReads / execTime;
        }

        double writeThroughPut() {
            return totalWrites * dataSize / execTime;
        }

        String getWriteConcern(){
            return namedConcerns.get(writeConcern);
        }

        @Override
        public String toString() {
            return String.format("%s,%s,%d,%d,%d,%d,%d,%1.0f,%s,%s",
                    remote,
                    sameDB,
                    noOfReaders,
                    noOfWriters,
                    totalReads,
                    totalWrites,
                    execTime,
                    readThroughPut(),
                    humanReadableByteCount((long) writeThroughPut()),
                    getWriteConcern());
        }
    }

    private static class Benchmark {
        private final MongoCollection<BasicDBObject> nodes;
        private final MongoCollection<BasicDBObject> blobs;
        private final Random random = new Random();
        private final AtomicBoolean stopTest = new AtomicBoolean(false);
        private final static byte[] DATA;
        private final CountDownLatch startLatch = new CountDownLatch(1);

        static {
            try {
                DATA = ByteStreams.toByteArray(new RandomStream(BLOB_SIZE, 100));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private Benchmark(MongoCollection<BasicDBObject> nodes,
                          MongoCollection<BasicDBObject> blobs) {
            this.nodes = nodes;
            this.blobs = blobs;
        }

        public Result run(int noOfReaders, int noOfWriters, boolean remote, WriteConcern writeConcern) throws InterruptedException {
            boolean sameDB = nodes.getNamespace().getDatabaseName().equals(blobs.getNamespace().getDatabaseName());

            List<Reader> readers = new ArrayList<Reader>(noOfReaders);
            List<Writer> writers = new ArrayList<Writer>(noOfWriters);
            List<Runnable> runnables = new ArrayList<Runnable>(noOfReaders + noOfWriters);
            final CountDownLatch stopLatch = new CountDownLatch(noOfReaders + noOfWriters);

            for (int i = 0; i < noOfReaders; i++) {
                readers.add(new Reader(stopLatch));
            }

            for (int i = 0; i < noOfWriters; i++) {
                writers.add(new Writer(i, stopLatch, writeConcern));
            }

            runnables.addAll(readers);
            runnables.addAll(writers);

            stopTest.set(false);

            List<Thread> threads = new ArrayList<Thread>();

            for (int i = 0; i < runnables.size(); i++) {
                Thread worker = new Thread(runnables.get(i));
                worker.start();
                threads.add(worker);
            }

            System.err.printf("Running with [%d] readers and [%d] writers. " +
                    "Same DB [%s], Remote server [%s], Max Time [%d] seconds, WriteConcern [%s] %n",
                    noOfReaders, noOfWriters, sameDB, remote, MAX_EXEC_TIME,namedConcerns.get(writeConcern));

            startLatch.countDown();

            TimeUnit.SECONDS.sleep(MAX_EXEC_TIME);
            stopTest.set(true);

            stopLatch.await();

            int totalReads = 0;
            for (Reader r : readers) {
                totalReads += r.readCount;
            }

            int totalWrites = 0;
            for (Writer w : writers) {
                totalWrites += w.writeCount;
            }

            Result r = new Result();
            r.execTime = MAX_EXEC_TIME;
            r.noOfReaders = noOfReaders;
            r.noOfWriters = noOfWriters;
            r.totalReads = totalReads;
            r.totalWrites = totalWrites;
            r.remote = remote;
            r.sameDB = sameDB;
            r.writeConcern = writeConcern;

            System.err.printf("Run complete. Reads [%d] and writes [%d] %n", totalReads, totalWrites);
            System.err.println(r.toString());
            return r;
        }

        private void waitForStart() {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        private class Reader implements Runnable {
            int readCount = 0;
            final CountDownLatch stopLatch;

            public Reader(CountDownLatch stopLatch) {
                this.stopLatch = stopLatch;
            }

            public void run() {
                waitForStart();
                while (!stopTest.get()) {
                    int id = random.nextInt(NO_OF_NODES);
                    BasicDBObject o = nodes.find(Filters.eq("_id", id)).first();
                    assertNotNull("did not found object with id " + id, o);
                    readCount++;
                }
                stopLatch.countDown();
            }
        }

        private class Writer implements Runnable {
            int writeCount = 0;
            final int id;
            final CountDownLatch stopLatch;
            final WriteConcern writeConcern;

            private Writer(int id, CountDownLatch stopLatch, WriteConcern writeConcern) {
                this.id = id;
                this.stopLatch = stopLatch;
                this.writeConcern = writeConcern;
            }

            public void run() {
                waitForStart();
                while (!stopTest.get()) {
                    String _id = id + "-" + writeCount;
                    BasicDBObject obj = new BasicDBObject()
                            .append("foo", _id);
                    obj.put("blob", DATA);
                    blobs.withWriteConcern(writeConcern).insertOne(obj);
                    writeCount++;
                }
                stopLatch.countDown();
            }
        }
    }
}