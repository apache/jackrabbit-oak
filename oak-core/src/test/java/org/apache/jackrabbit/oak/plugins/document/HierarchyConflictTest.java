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
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.fail;

/**
 * Test for OAK-2151
 */
@Ignore
public class HierarchyConflictTest {

    @Test
    public void addRemove() throws Throwable {
        final List<Throwable> exceptions = Lists.newArrayList();
        final CountDownLatch nodeRemoved = new CountDownLatch(1);
        final CountDownLatch nodeAdded = new CountDownLatch(1);

        final DocumentNodeStore store = new DocumentMK.Builder().getNodeStore();
        NodeBuilder root = store.getRoot().builder();
        root.child("foo").child("bar").child("baz");
        merge(store, root, null);

        NodeBuilder r1 = store.getRoot().builder();
        r1.child("addNode");

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                NodeBuilder r2 = store.getRoot().builder();
                r2.child("removeNode");

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

        try {
            // must fail because /foo/bar was removed
            merge(store, r1, new EditorCallback() {
                @Override
                public void edit(NodeBuilder builder) {
                    awaitUninterruptibly(nodeRemoved);
                    builder.getChildNode("foo").getChildNode("bar").child("qux");
                    nodeAdded.countDown();
                }
            });
            joinUninterruptibly(t);

            // this is just for debug purpose and will only run when
            // the expected exception does not occur
            DocumentStore ds = store.getDocumentStore();
            store.runBackgroundOperations();
            Revision head = store.getHeadRevision();

            NodeDocument foo = ds.find(NODES, getIdFromPath("/foo"));
            NodeDocument bar = ds.find(NODES, getIdFromPath("/foo/bar"));
            NodeDocument qux = ds.find(NODES, getIdFromPath("/foo/bar/qux"));

            NodeState state = foo.getNodeAtRevision(store, head, null);
            System.out.println("foo : " + state);
            state = bar.getNodeAtRevision(store, head, null);
            System.out.println("bar : " + state);
            state = qux.getNodeAtRevision(store, head, null);
            System.out.println("qux : " + state);

            System.out.println(ds);


            fail("Must fail with CommitFailedException. Cannot add child node" +
                    " to a removed parent");
        } catch (CommitFailedException e) {
            // expected
            System.out.println("expected: " + e.toString());
        }

        for (Throwable ex : exceptions) {
            throw ex;
        }

    }

    private static void merge(NodeStore store,
                              NodeBuilder root,
                              final EditorCallback callback)
            throws CommitFailedException {
        CompositeHook hooks = new CompositeHook(
                new EditorHook(new EditorProvider() {
                    @Override
                    public Editor getRootEditor(NodeState before,
                                                NodeState after,
                                                NodeBuilder builder,
                                                CommitInfo info)
                            throws CommitFailedException {
                        if (callback != null) {
                            callback.edit(builder);
                        }
                        return null;
                    }
                }),
                new ConflictHook(new AnnotatingConflictHandler()),
                new EditorHook(new ConflictValidatorProvider()));
        store.merge(root, hooks, CommitInfo.EMPTY);
    }

    private interface EditorCallback {

        public void edit(NodeBuilder builder);

    }
}
