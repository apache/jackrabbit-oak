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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ClusterConflictTest {

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private static final Logger LOG = LoggerFactory.getLogger(ClusterConflictTest.class);

    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    @Before
    public void setUp() {
        MemoryDocumentStore store = new MemoryDocumentStore();
        ns1 = newDocumentNodeStore(store, 1);
        ns2 = newDocumentNodeStore(store, 2);
    }

    private DocumentNodeStore newDocumentNodeStore(DocumentStore store, int clusterId) {
        // use high async delay and run background ops manually
        // asyncDelay set to zero prevents commits from suspending
        return builderProvider.newBuilder()
                .setAsyncDelay(60000)
                .setDocumentStore(store)
                .setLeaseCheck(false) // disabled for debugging purposes
                .setClusterId(clusterId)
                .getNodeStore();
    }

    @Test
    public void suspendUntilVisible() throws Exception {
        suspendUntilVisible(false);
    }

    @Test
    public void suspendUntilVisibleWithBranch() throws Exception {
        suspendUntilVisible(true);
    }

    private void suspendUntilVisible(boolean withBranch) throws Exception {
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("counter").setProperty("value", 0);
        merge(ns1, b1);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        b1 = ns1.getRoot().builder();
        b1.child("foo");
        ns1.merge(b1, new TestHook(), EMPTY);

        final List<Exception> exceptions = Lists.newArrayList();
        final NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("bar");
        if (withBranch) {
            purge(b2);
        }
        b2.child("baz");

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("initiating merge");
                    ns2.merge(b2, new TestHook(), EMPTY);
                    LOG.info("merge succeeded");
                } catch (CommitFailedException e) {
                    exceptions.add(e);
                }
            }
        });
        t.start();

        // wait until t is suspended
        for (int i = 0; i < 100; i++) {
            if (ns2.commitQueue.numSuspendedThreads() > 0) {
                break;
            }
            Thread.sleep(10);
        }
        assertEquals(1, ns2.commitQueue.numSuspendedThreads());
        LOG.info("commit suspended");

        ns1.runBackgroundOperations();
        LOG.info("ran background ops on ns1");
        ns2.runBackgroundOperations();
        LOG.info("ran background ops on ns2");
        assertEquals(0, ns2.commitQueue.numSuspendedThreads());

        t.join(3000);
        assertFalse("Commit did not succeed within 3 seconds", t.isAlive());

        for (Exception e : exceptions) {
            throw e;
        }
    }

    // OAK-3859
    @Test
    public void mixedConflictAndCollision() throws Exception {
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("test");
        merge(ns1, b1);

        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        AtomicLong counter = new AtomicLong();
        final List<Exception> exceptions = Collections.synchronizedList(
                new ArrayList<Exception>());
        // the writers perform conflicting changes
        List<Thread> writers = Lists.newArrayList();
        writers.add(new Thread(new Writer(exceptions, ns1, counter)));
        writers.add(new Thread(new Writer(exceptions, ns1, counter)));
        for (Thread t : writers) {
            t.start();
        }

        for (int i = 0; i < 10; i++) {
            NodeBuilder b21 = ns2.getRoot().builder();
            // this change does not conflict with changes on ns1 but
            // will be considered a collision
            b21.child("test").setProperty("q", 1);
            merge(ns2, b21);
        }

        for (Thread t : writers) {
            t.join(10000);
        }

        for (Exception e : exceptions) {
            throw e;
        }
    }

    private static class Writer implements Runnable {

        private final List<Exception> exceptions;
        private final NodeStore ns;
        private final AtomicLong counter;

        public Writer(List<Exception> exceptions,
                      NodeStore ns,
                      AtomicLong counter) {
            this.exceptions = exceptions;
            this.ns = ns;
            this.counter = counter;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 200; i++) {
                    NodeBuilder b = ns.getRoot().builder();
                    b.child("test").setProperty("p", counter.incrementAndGet());
                    merge(ns, b);
                }
            } catch (CommitFailedException e) {
                e.printStackTrace();
                exceptions.add(e);
            }
        }
    }

    private static class TestHook extends EditorHook {

        TestHook() {
            super(new EditorProvider() {
                @CheckForNull
                @Override
                public Editor getRootEditor(NodeState before,
                                            NodeState after,
                                            NodeBuilder builder,
                                            CommitInfo info)
                        throws CommitFailedException {
                    return new TestEditor(builder.child("counter"));
                }
            });
        }
    }

    private static class TestEditor extends DefaultEditor {

        private NodeBuilder counter;

        TestEditor(NodeBuilder counter) {
            this.counter = counter;
        }

        @Override
        public Editor childNodeAdded(String name, NodeState after)
                throws CommitFailedException {
            counter.setProperty("value", counter.getProperty("value").getValue(Type.LONG) + 1);
            return super.childNodeAdded(name, after);
        }
    }

    private static NodeState merge(NodeStore store, NodeBuilder root)
            throws CommitFailedException {
        return store.merge(root, EmptyHook.INSTANCE, EMPTY);
    }

    private static void purge(NodeBuilder builder) {
        ((DocumentRootBuilder) builder).purge();
    }
}
