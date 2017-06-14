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
import java.util.concurrent.CountDownLatch;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;
import static org.junit.Assert.fail;

/**
 * Test for OAK-2151
 */
public class HierarchyConflictTest {

    private static final Logger LOG = LoggerFactory.getLogger(HierarchyConflictTest.class);

    private List<Throwable> exceptions;
    private CountDownLatch nodeRemoved;
    private CountDownLatch nodeAdded;
    private DocumentNodeStore store;

    @Before
    public void before() {
        exceptions = Lists.newArrayList();
        nodeRemoved = new CountDownLatch(1);
        nodeAdded = new CountDownLatch(1);
        store = new DocumentMK.Builder().getNodeStore();
    }

    @After
    public void after() {
        store.dispose();
    }

    @Test
    public void conflict() throws Throwable {
        NodeBuilder root = store.getRoot().builder();
        root.child("foo").child("bar").child("baz");
        merge(store, root, null);

        NodeBuilder r1 = store.getRoot().builder();
        r1.child("addNode");

        final NodeBuilder r2 = store.getRoot().builder();
        r2.child("removeNode");

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    merge(store, r2, new EditorCallback() {
                        @Override
                        public void edit(NodeBuilder builder) {
                            builder.getChildNode("foo").getChildNode("bar").remove();
                            nodeRemoved.countDown();
                            awaitUninterruptibly(nodeAdded);
                        }
                    });
                } catch (CommitFailedException e) {
                    exceptions.add(e);
                }
            }
        });
        t.start();

        // wait for r2 to enter merge phase
        awaitUninterruptibly(nodeRemoved);
        try {
            // must fail because /foo/bar was removed
            merge(store, r1, new EditorCallback() {
                @Override
                public void edit(NodeBuilder builder) {
                    builder.getChildNode("foo").getChildNode("bar").child("qux");
                    nodeAdded.countDown();
                    // wait until r2 commits
                    joinUninterruptibly(t);
                }
            });

            for (Throwable ex : exceptions) {
                fail(ex.toString());
            }

            fail("Must fail with CommitFailedException. Cannot add child node" +
                    " to a removed parent");
        } catch (CommitFailedException e) {
            // expected
            LOG.info("expected: {}", e.toString());
        }
    }


    @Test
    public void conflict2() throws Throwable {
        NodeBuilder root = store.getRoot().builder();
        root.child("foo").child("bar").child("baz");
        merge(store, root, null);

        final NodeBuilder r1 = store.getRoot().builder();
        r1.child("addNode");

        NodeBuilder r2 = store.getRoot().builder();
        r2.child("removeNode");

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    merge(store, r1, new EditorCallback() {
                        @Override
                        public void edit(NodeBuilder builder) {
                            builder.getChildNode("foo").getChildNode("bar").child("qux");
                            nodeAdded.countDown();
                            awaitUninterruptibly(nodeRemoved);
                        }
                    });
                } catch (CommitFailedException e) {
                    exceptions.add(e);
                }
            }
        });
        t.start();

        // wait for r1 to enter merge phase
        awaitUninterruptibly(nodeAdded);
        try {
            // must fail because /foo/bar/qux was added
            merge(store, r2, new EditorCallback() {
                @Override
                public void edit(NodeBuilder builder) {
                    builder.getChildNode("foo").getChildNode("bar").remove();
                    nodeRemoved.countDown();
                    // wait until r1 commits
                    joinUninterruptibly(t);
                }
            });

            for (Throwable ex : exceptions) {
                fail(ex.toString());
            }

            fail("Must fail with CommitFailedException. Cannot remove tree" +
                    " when child is added concurrently");
        } catch (CommitFailedException e) {
            // expected
            LOG.info("expected: {}", e.toString());
        }
    }

    private static void merge(NodeStore store,
                              NodeBuilder root,
                              final EditorCallback callback)
            throws CommitFailedException {
        CompositeHook hooks = new CompositeHook(
                new EditorHook(new EditorProvider() {
                    private int numEdits = 0;
                    @Override
                    public Editor getRootEditor(NodeState before,
                                                NodeState after,
                                                NodeBuilder builder,
                                                CommitInfo info)
                            throws CommitFailedException {
                        if (callback != null) {
                            if (++numEdits > 1) {
                                // this is a retry, fail the commit
                                throw new CommitFailedException(OAK, 0,
                                        "do not retry merge in this test");
                            }
                            callback.edit(builder);
                        }
                        return null;
                    }
                }),
                ConflictHook.of(new AnnotatingConflictHandler()),
                new EditorHook(new ConflictValidatorProvider()));
        store.merge(root, hooks, CommitInfo.EMPTY);
    }

    private interface EditorCallback {

        public void edit(NodeBuilder builder) throws CommitFailedException;

    }
}
