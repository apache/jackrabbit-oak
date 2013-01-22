/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk.impl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.AbstractMongoConnectionTest;
import org.apache.jackrabbit.mongomk.impl.blob.MongoGridFSBlobStore;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.DB;

/**
 * Test for OAK-566.
 */
public class MongoMKConcurrentAddTest extends AbstractMongoConnectionTest {

    private static final int NB_THREADS = 16;

    private List<MongoConnection> connections = new ArrayList<MongoConnection>();

    private MicroKernel createMicroKernel() throws Exception {
        MongoConnection connection = new MongoConnection(HOST,
                PORT, DB);
        connections.add(connection);
        DB mongoDB = connection.getDB();
        MongoNodeStore nodeStore = new MongoNodeStore(mongoDB);
        MongoGridFSBlobStore blobStore = new MongoGridFSBlobStore(mongoDB);
        return new MongoMicroKernel(connection, nodeStore, blobStore);
    }

    @After
    public void closeConnections() {
        for (MongoConnection mc : connections) {
            mc.close();
        }
        connections.clear();
    }

    /**
     * Creates NB_THREADS microkernels, each commiting two nodes (one parent,
     * one child) in its own thread. The nodes being committed by separate
     * threads do not overlap / conflict.
     */
    @Test
    @Ignore("OAK-566")
    public void testConcurrentAdd() throws Exception {
        // create workers
        List<Callable<String>> cs = new LinkedList<Callable<String>>();
        for (int i = 0; i < NB_THREADS; i++) {
            // each callable has its own microkernel
            final MicroKernel mk = createMicroKernel();
            // diff for adding one node and one child node
            final List<String> stmts = new LinkedList<String>();
            stmts.add("+\"node" + i + "\":{}");
            stmts.add("+\"node" + i + "/child\":{}");
            // create callable
            Callable<String> c = new Callable<String>() {
                @Override
                public String call() throws Exception {
                    // commit all statements, one at a time
                    String r = null;
                    for (String stmt : stmts) {
                        r = mk.commit("/", stmt, null, "msg");
                    }
                    return r;
                }
            };
            cs.add(c);
        }

        // run workers concurrently
        ExecutorService executor = Executors.newFixedThreadPool(NB_THREADS);
        List<Future<String>> fs = new LinkedList<Future<String>>();
        for (Callable<String> c : cs) {
            fs.add(executor.submit(c));
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        // get all results in order to verify if any of the threads has thrown
        // an exception
        for (Future<String> f : fs) {
            f.get();
        }
    }
}
