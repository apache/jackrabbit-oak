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
package org.apache.jackrabbit.mk.store;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.core.Repository;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.persistence.GCPersistence;
import org.apache.jackrabbit.mk.persistence.InMemPersistence;
import org.apache.jackrabbit.mk.store.DefaultRevisionStore.PutTokenImpl;
import org.apache.jackrabbit.mk.store.RevisionStore.PutToken;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests verifying the inner workings of <code>DefaultRevisionStore</code>.
 */
public class DefaultRevisionStoreTest {

    /* avoid synthetic accessor */  DefaultRevisionStore rs;
    private MicroKernelImpl mk;
    
    @Before
    public void setup() throws Exception {
        rs = new DefaultRevisionStore(createPersistence()) {
            @Override
            protected Id markCommits() throws Exception {
                // Keep head commit only
                StoredCommit commit = getHeadCommit();
                markCommit(commit);
                return commit.getId();
            }
        };
        rs.initialize();

        mk = new MicroKernelImpl(new Repository(rs, new MemoryBlobStore()));
    }
    
    protected GCPersistence createPersistence() throws Exception {
        return new InMemPersistence();
    }

    @After
    public void tearDown() throws Exception {
        if (mk != null) {
            mk.dispose();
        }
    }
    
    /**
     * Verify revision history works with garbage collection.
     * 
     * @throws Exception if an error occurs
     */
    @Test
    public void testRevisionHistory() {
        mk.commit("/", "+\"a\" : { \"c\":{}, \"d\":{} }", mk.getHeadRevision(), null);
        mk.commit("/", "+\"b\" : {}", mk.getHeadRevision(), null);
        mk.commit("/b", "+\"e\" : {}", mk.getHeadRevision(), null);
        mk.commit("/a/c", "+\"f\" : {}", mk.getHeadRevision(), null);
        
        String headRevision = mk.getHeadRevision();
        String contents = mk.getNodes("/", headRevision, 1, 0, -1, null);

        rs.gc();
        
        assertEquals(headRevision, mk.getHeadRevision());
        assertEquals(contents, mk.getNodes("/", headRevision, 1, 0, -1, null));
        
        String history = mk.getRevisionHistory(Long.MIN_VALUE, Integer.MIN_VALUE, null);
        assertEquals(1, parseJSONArray(history).size());
    }

    /**
     * Verify branch and merge works with garbage collection.
     * 
     * @throws Exception if an error occurs
     */
    @Test
    public void testBranchMerge() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\":{}, \"c\":{} }", mk.getHeadRevision(), null);
        String branchRevisionId = mk.branch(mk.getHeadRevision());

        mk.commit("/a", "+\"d\" : {}", mk.getHeadRevision(), null);
        branchRevisionId = mk.commit("/a", "+\"e\" : {}", branchRevisionId, null);
        
        rs.gc();

        branchRevisionId = mk.commit("/a", "+\"f\" : {}", branchRevisionId, null);
        mk.merge(branchRevisionId, null);

        rs.gc();

        String history = mk.getRevisionHistory(Long.MIN_VALUE, Integer.MIN_VALUE, null);
        assertEquals(1, parseJSONArray(history).size());
    }
    
    /**
     * Verify garbage collection can run concurrently with commits.
     * 
     * @throws Exception if an error occurs
     */
    @Test
    public void testConcurrentGC() throws Exception {
        ScheduledExecutorService gcExecutor = Executors.newScheduledThreadPool(1);
        gcExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                rs.gc();
            }
        }, 100, 20, TimeUnit.MILLISECONDS);

        mk.commit("/", "+\"a\" : { \"b\" : { \"c\" : { \"d\" : {} } } }",
                mk.getHeadRevision(), null);

        try {
            for (int i = 0; i < 20; i++) {
                mk.commit("/a/b/c/d", "+\"e\" : {}", mk.getHeadRevision(), null);
                Thread.sleep(10);
                mk.commit("/a/b/c/d/e", "+\"f\" : {}", mk.getHeadRevision(), null);
                Thread.sleep(30);
                mk.commit("/a/b/c/d", "-\"e\"", mk.getHeadRevision(), null);
            }
        } finally {
            gcExecutor.shutdown();
        }
    }

    /**
     * Verify garbage collection can run concurrently with branch & merge.
     * 
     * @throws Exception if an error occurs
     */
    @Test
    @Ignore
    public void testConcurrentMergeGC() throws Exception {
        ScheduledExecutorService gcExecutor = Executors.newScheduledThreadPool(1);
        gcExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                rs.gc();
            }
        }, 100, 20, TimeUnit.MILLISECONDS);

        mk.commit("/", "+\"a\" : { \"b\" : { \"c\" : { \"d\" : {} } } }",
                mk.getHeadRevision(), null);

        try {
            for (int i = 0; i < 20; i++) {
                String branchId = mk.branch(mk.getHeadRevision());
                if ((i & 1) == 0) {
                    /* add some data in even runs */
                    branchId = mk.commit("/a/b/c/d", "+\"e\" : {}", branchId, null);
                    Thread.sleep(10);
                    branchId = mk.commit("/a/b/c/d/e", "+\"f\" : {}", branchId, null);
                } else {
                    /* remove added data in odd runs */
                    branchId = mk.commit("/a/b/c/d", "-\"e\"", branchId, null);
                }
                Thread.sleep(30);
                mk.merge(branchId, null);
            }
        } finally {
            gcExecutor.shutdown();
        }
    }

    @Test
    @Ignore
    public void putTokenImpl() throws InterruptedException, ExecutionException {
        final Set<PutToken> tokens = Collections.synchronizedSet(new HashSet<PutToken>());
        Set<Future<?>> results = new HashSet<Future<?>>();

        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 100; i++) {
            results.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int j = 0; j < 10000; j++) {
                        assertTrue(tokens.add(new PutTokenImpl()));
                    }
                    return null;
                }
            }));
        }

        for (Future<?> result : results) {
            result.get();
        }
    }

    /**
     * Parses the provided string into a {@code JSONArray}.
     *
     * @param json string to be parsed
     * @return a {@code JSONArray}
     * @throws {@code AssertionError} if the string cannot be parsed into a {@code JSONArray}
     */
    private JSONArray parseJSONArray(String json) throws AssertionError {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(json);
            assertTrue(obj instanceof JSONArray);
            return (JSONArray) obj;
        } catch (Exception e) {
            throw new AssertionError("not a valid JSON array: " + e.getMessage());
        }
    }
}
