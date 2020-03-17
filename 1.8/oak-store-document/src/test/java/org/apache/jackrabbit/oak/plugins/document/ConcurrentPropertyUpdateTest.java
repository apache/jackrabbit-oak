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
package org.apache.jackrabbit.oak.plugins.document;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Test;

import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Test for OAK-1751.
 */
public class ConcurrentPropertyUpdateTest extends BaseDocumentMKTest {

    private static final int NUM_THREADS = 2;

    private static final CommitHook HOOK = new CompositeHook(
            ConflictHook.of(new AnnotatingConflictHandler()),
            new EditorHook(new ConflictValidatorProvider()));

    private ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);

    @After
    public void dispose() {
        service.shutdown();
    }

    @Override
    public void initDocumentMK() {
        mk = new DocumentMK.Builder().setDocumentStore(new MemoryDocumentStore() {
            @Override
            public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                        UpdateOp update)
                    throws DocumentStoreException {
                try {
                    Thread.sleep((long) (Math.random() * 10f));
                } catch (InterruptedException e) {
                    // ignore
                }
                return super.findAndUpdate(collection, update);
            }
        }).open();
    }

    @Test
    public void concurrentUpdates() throws Exception {
        final DocumentNodeStore store = mk.getNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").setProperty("prop", System.currentTimeMillis());
        store.merge(builder, HOOK, CommitInfo.EMPTY);
        List<Callable<Object>> tasks = Lists.newArrayList();
        for (int i = 0; i < NUM_THREADS; i++) {
            tasks.add(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    for (int i = 0; i < 100; i++) {
                        try {
                            NodeBuilder builder = store.getRoot().builder();
                            builder.getChildNode("test").setProperty(
                                    "prop", Math.random());
                            store.merge(builder, HOOK, CommitInfo.EMPTY);
                        } catch (CommitFailedException e) {
                            // merge must either succeed or fail
                            // with an InvalidItemStateException
                            RepositoryException ex = e.asRepositoryException();
                            if (!(ex instanceof InvalidItemStateException)) {
                                throw e;
                            }
                        }
                    }
                    return null;
                }
            });
        }
        List<Future<Object>> results = service.invokeAll(tasks);
        for (Future<Object> r : results) {
            r.get();
        }
    }
}
