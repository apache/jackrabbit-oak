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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConcurrentReadAndAddTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

    private DocumentNodeStore ns;
    private volatile boolean delayQuery = false;
    private Thread main;

    @Before
    public void setUp() throws Exception {
        ns = builderProvider.newBuilder().setDocumentStore(new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      int limit) {
                List<T> docs = super.query(collection, fromKey, toKey, limit);
                if (delayQuery && Thread.currentThread() == main) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                return docs;
            }
        }).getNodeStore();
    }

    @Test
    public void concurrentReadAdd() throws Exception {
        main = Thread.currentThread();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        merge(builder);

        final Semaphore sem = new Semaphore(0);
        Thread u = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    addNode(i);
                    if (i == 20) {
                        sem.release();
                    }
                }
            }
        });
        u.setName("writer");
        u.start();

        sem.acquireUninterruptibly();
        readNodes();

        u.join();

        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
        NodeState test = ns.getRoot().getChildNode("test");
        assertEquals(100, Iterables.size(test.getChildNodeEntries()));
    }

    private void readNodes() {
        delayQuery = true;
        NodeState test = ns.getRoot().getChildNode("test");
        for (ChildNodeEntry entry : test.getChildNodeEntries()) {
            entry.getNodeState();
        }
    }

    private void addNode(int i) {
        try {
            NodeBuilder builder = ns.getRoot().builder();
            builder.child("test").child("node-" + i);
            merge(builder);
        } catch (CommitFailedException e) {
            exceptions.add(e);
        }
    }

    private void merge(NodeBuilder builder)
            throws CommitFailedException {
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
