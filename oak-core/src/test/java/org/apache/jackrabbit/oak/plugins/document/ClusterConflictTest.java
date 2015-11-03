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

import java.util.List;

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
        ns1 = newDocumentNodeStore(store);
        ns2 = newDocumentNodeStore(store);
    }

    private DocumentNodeStore newDocumentNodeStore(DocumentStore store) {
        // use high async delay and run background ops manually
        // asyncDelay set to zero prevents commits from suspending
        return builderProvider.newBuilder()
                .setAsyncDelay(60000)
                .setDocumentStore(store)
                .setLeaseCheck(false) // disabled for debugging purposes
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
