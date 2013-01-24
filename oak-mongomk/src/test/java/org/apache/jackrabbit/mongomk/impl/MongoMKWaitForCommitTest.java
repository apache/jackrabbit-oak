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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.blob.MongoGridFSBlobStore;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DB;

/**
 * Tests for {@code MongoMicroKernel#waitForCommit(String, long)}
 */
public class MongoMKWaitForCommitTest extends BaseMongoMicroKernelTest {

    private MicroKernel mk2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DB db = mongoConnection.getDB();
        NodeStore nodeStore = new MongoNodeStore(db);
        BlobStore blobStore = new MongoGridFSBlobStore(db);
        mk2 = new MongoMicroKernel(mongoConnection, nodeStore, blobStore);
    }

    @Test
    public void timeoutNonPositiveNoCommit() throws Exception {
        String headRev = mk.commit("/", "+\"a\" : {}", null, null);
        long before = System.currentTimeMillis();
        String rev = mk2.waitForCommit(null, -1);
        long after = System.currentTimeMillis();
        assertEquals(headRev, rev);
        assertTrue(after - before < 100); // Basically no wait.
    }

    @Test
    public void timeoutNoCommit() throws Exception {
        int timeout = 500;
        String headRev = mk.commit("/", "+\"a\" : {}", null, null);
        long before = System.currentTimeMillis();
        String rev = mk2.waitForCommit(headRev, timeout);
        long after = System.currentTimeMillis();
        assertEquals(headRev, rev);
        assertTrue(after - before >= timeout);
    }

    @Test
    public void timeoutWithCommit1() throws Exception {
        String headRev = mk.commit("/", "+\"a\" : {}", null, null);
        ScheduledFuture<String> future = scheduleCommit(1000, null);
        int timeout = 500;
        long before = System.currentTimeMillis();
        String rev = mk2.waitForCommit(headRev, timeout);
        long after = System.currentTimeMillis();
        headRev = future.get();
        assertFalse(headRev.equals(rev));
        assertTrue(after - before >= timeout);
    }

    @Test
    public void timeoutWithCommit2() throws Exception {
        String headRev = mk.commit("/", "+\"a\" : {}", null, null);
        ScheduledFuture<String> future = scheduleCommit(500, null);
        int timeout = 2000;
        long before = System.currentTimeMillis();
        String rev = mk2.waitForCommit(headRev, timeout);
        long after = System.currentTimeMillis();
        headRev = future.get();
        assertTrue(headRev.equals(rev));
        assertTrue(after - before < timeout);
    }

    @Test
    public void branchIgnored() throws Exception {
        String headRev = mk.commit("/", "+\"a\" : {}", null, null);
        String branchRev = mk.branch(headRev);
        ScheduledFuture<String> future = scheduleCommit(500, branchRev);
        int timeout = 2000;
        long before = System.currentTimeMillis();
        String rev = mk2.waitForCommit(headRev, timeout);
        long after = System.currentTimeMillis();
        headRev = future.get();
        assertFalse(headRev.equals(rev));
        assertTrue(after - before >= timeout);
    }

    @Test
    public void nullOldHeadRevisionId() throws Exception {
        String headRev = mk.commit("/", "+\"a\" : {}", null, null);
        long before = System.currentTimeMillis();
        String rev = mk2.waitForCommit(null, 500);
        long after = System.currentTimeMillis();
        assertEquals(headRev, rev);
        assertEquals(headRev, rev);
        assertTrue(after - before < 10); // Basically no wait.
    }

    private ScheduledFuture<String> scheduleCommit(long delay, final String revisionId) {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<String> future = executorService.schedule(new Callable<String>(){
            @Override
            public String call() throws Exception {
                return mk.commit("/", "+\"b\" : {}", revisionId, null);
            }
        }, delay, TimeUnit.MILLISECONDS);
        executorService.shutdown();
        return future;
    }
}