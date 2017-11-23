/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.segment.WriteOperationHandler.WriteOperation;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.After;
import org.junit.Test;

public class SegmentBufferWriterPoolTest {
    private final MemoryStore store = new MemoryStore();

    private final RecordId rootId = store.getRevisions().getHead();

    private final SegmentBufferWriterPool pool = new SegmentBufferWriterPool(
            store.getSegmentIdProvider(),
            store.getReader(),
            "",
            Suppliers.ofInstance(GCGeneration.NULL)
    );

    private final ExecutorService[] executors = new ExecutorService[] {
        newSingleThreadExecutor(), newSingleThreadExecutor(), newSingleThreadExecutor()};

    public SegmentBufferWriterPoolTest() throws IOException { }

    @After
    public void tearDown() {
        for (ExecutorService executor : executors) {
            executor.shutdown();
        }
    }

    private Future<RecordId> execute(final WriteOperation op, int executor) {
        return executors[executor].submit(new Callable<RecordId>() {
            @Override
            public RecordId call() throws Exception {
                return pool.execute(op);
            }
        });
    }

    private WriteOperation createOp(final String key, final ConcurrentMap<String, SegmentBufferWriter> map) {
        return new WriteOperation() {
            @Nonnull @Override
            public RecordId execute(@Nonnull SegmentBufferWriter writer) {
                map.put(key, writer);
                return rootId;
            }
        };
    }

    @Test
    public void testThreadAffinity() throws IOException, ExecutionException, InterruptedException {
        ConcurrentMap<String, SegmentBufferWriter> map1 = newConcurrentMap();
        Future<RecordId> res1 = execute(createOp("a", map1), 0);
        Future<RecordId> res2 = execute(createOp("b", map1), 1);
        Future<RecordId> res3 = execute(createOp("c", map1), 2);

        // Give the tasks some time to complete
        sleepUninterruptibly(10, MILLISECONDS);

        assertEquals(rootId, res1.get());
        assertEquals(rootId, res2.get());
        assertEquals(rootId, res3.get());
        assertEquals(3, map1.size());

        ConcurrentMap<String, SegmentBufferWriter> map2 = newConcurrentMap();
        Future<RecordId> res4 = execute(createOp("a", map2), 0);
        Future<RecordId> res5 = execute(createOp("b", map2), 1);
        Future<RecordId> res6 = execute(createOp("c", map2), 2);

        // Give the tasks some time to complete
        sleepUninterruptibly(10, MILLISECONDS);

        assertEquals(rootId, res4.get());
        assertEquals(rootId, res5.get());
        assertEquals(rootId, res6.get());
        assertEquals(3, map2.size());
        assertEquals(map1, map2);
    }

    @Test
    public void testFlush() throws ExecutionException, InterruptedException, IOException {
        ConcurrentMap<String, SegmentBufferWriter> map1 = newConcurrentMap();
        Future<RecordId> res1 = execute(createOp("a", map1), 0);
        Future<RecordId> res2 = execute(createOp("b", map1), 1);
        Future<RecordId> res3 = execute(createOp("c", map1), 2);

        // Give the tasks some time to complete
        sleepUninterruptibly(10, MILLISECONDS);

        assertEquals(rootId, res1.get());
        assertEquals(rootId, res2.get());
        assertEquals(rootId, res3.get());
        assertEquals(3, map1.size());

        pool.flush(store);

        ConcurrentMap<String, SegmentBufferWriter> map2 = newConcurrentMap();
        Future<RecordId> res4 = execute(createOp("a", map2), 0);
        Future<RecordId> res5 = execute(createOp("b", map2), 1);
        Future<RecordId> res6 = execute(createOp("c", map2), 2);

        // Give the tasks some time to complete
        sleepUninterruptibly(10, MILLISECONDS);

        assertEquals(rootId, res4.get());
        assertEquals(rootId, res5.get());
        assertEquals(rootId, res6.get());
        assertEquals(3, map2.size());
        assertTrue(intersection(newHashSet(map1.values()), newHashSet(map2.values())).isEmpty());
    }

    @Test
    public void testFlushBlocks() throws ExecutionException, InterruptedException {
        Future<RecordId> res = execute(new WriteOperation() {
            @Nonnull
            @CheckForNull @Override
            public RecordId execute(@Nonnull SegmentBufferWriter writer) {
                try {
                    // This should deadlock as flush waits for this write
                    // operation to finish, which in this case contains the
                    // call to flush itself.
                    executors[1].submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            pool.flush(store);
                            return null;
                        }
                    }).get(100, MILLISECONDS);
                    return null;    // No deadlock -> null indicates test failure
                } catch (InterruptedException | ExecutionException ignore) {
                    return null;    // No deadlock -> null indicates test failure
                } catch (TimeoutException ignore) {
                    return rootId;  // Deadlock -> rootId indicates test pass
                }
            }
        }, 0);

        assertEquals(rootId, res.get());
    }

}
