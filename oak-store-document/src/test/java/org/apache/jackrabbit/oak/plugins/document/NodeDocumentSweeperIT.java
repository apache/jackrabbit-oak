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
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoMissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBMissingLastRevSeeker;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.disposeQuietly;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NodeDocumentSweeperIT extends AbstractTwoNodeTest {

    private static final Path BASE_PATH = Path.fromString("/foo/bar/baz");

    private static final Path TEST_PATH = new Path(BASE_PATH, "test");

    private FailingDocumentStore fds1;

    private LastRevRecoveryAgent agent2;

    public NodeDocumentSweeperIT(DocumentStoreFixture fixture) {
        super(fixture);
    }

    @Override
    protected DocumentStore customize(DocumentStore store) {
        if (fds1 == null) {
            fds1 = new FailingDocumentStore(store);
            return fds1;
        } else {
            return store;
        }
    }

    @Before
    public void prepareAgent() {
        // first setup seeker according to underlying document store implementation
        MissingLastRevSeeker seeker;
        if (store2 instanceof MongoDocumentStore) {
            seeker = new MongoMissingLastRevSeeker((MongoDocumentStore) store2, clock);
        } else if (store2 instanceof RDBDocumentStore) {
            seeker = new RDBMissingLastRevSeeker((RDBDocumentStore) store2, clock) {
                @Override
                public @NotNull Iterable<NodeDocument> getCandidates(long startTime) {
                    List<NodeDocument> docs = new ArrayList<>();
                    super.getCandidates(startTime).forEach(docs::add);
                    docs.sort((o1, o2) -> NodeDocumentIdComparator.INSTANCE.compare(o1.getId(), o2.getId()));
                    return docs;
                }
            };
        } else {
            // use default implementation
            seeker = new MissingLastRevSeeker(store2, clock);
        }
        // then customize seeker to return documents in a defined order
        // return docs sorted by decreasing depth
        MissingLastRevSeeker testSeeker = new MissingLastRevSeeker(store2, clock) {
            @Override
            public @NotNull Iterable<NodeDocument> getCandidates(long startTime) {
                List<NodeDocument> docs = new ArrayList<>();
                seeker.getCandidates(startTime).forEach(docs::add);
                docs.sort((o1, o2) -> NodeDocumentIdComparator.INSTANCE.compare(o1.getId(), o2.getId()));
                return docs;
            }
        };
        agent2 = new LastRevRecoveryAgent(ds2.getDocumentStore(), ds2, testSeeker, v -> {});
    }

    @Test
    public void recoveryWithSweepNodeAdded() throws Exception {
        // create some test data
        NodeBuilder builder = ds2.getRoot().builder();
        getOrCreate(builder, BASE_PATH);
        merge(ds2, builder);
        ds2.runBackgroundOperations();
        ds1.runBackgroundOperations();
        // now these nodes are visible on ds1
        assertExists(ds1, BASE_PATH);
        // wait a bit
        clock.waitUntil(clock.getTime() + SECONDS.toMillis(10));
        // add a child
        builder = ds1.getRoot().builder();
        getOrCreate(builder, TEST_PATH);
        merge(ds1, builder);
        // simulate a crash
        fds1.fail().after(0).eternally();
        disposeQuietly(ds1);
        // wait and run recovery for ds1
        clock.waitUntil(clock.getTime() + MINUTES.toMillis(3));
        ds2.renewClusterIdLease();

        assertTrue(agent2.isRecoveryNeeded());
        agent2.recover(1);
        ds2.runBackgroundOperations();
        assertExists(ds2, TEST_PATH);
    }

    @Test
    public void recoveryWithSweepNodeDeleted() throws Exception {
        // create some test data
        NodeBuilder builder = ds2.getRoot().builder();
        getOrCreate(builder, TEST_PATH);
        merge(ds2, builder);
        ds2.runBackgroundOperations();
        ds1.runBackgroundOperations();
        // now these nodes are visible on ds1
        assertExists(ds1, TEST_PATH);
        // wait a bit
        clock.waitUntil(clock.getTime() + SECONDS.toMillis(10));
        // remove the child
        builder = ds1.getRoot().builder();
        getOrCreate(builder, TEST_PATH).remove();
        // modify something on the remaining parent to move the commit root there
        getOrCreate(builder, BASE_PATH).setProperty("p", "v");
        merge(ds1, builder);
        // simulate a crash
        fds1.fail().after(0).eternally();
        disposeQuietly(ds1);
        // wait and run recovery for ds1
        clock.waitUntil(clock.getTime() + MINUTES.toMillis(3));
        ds2.renewClusterIdLease();

        assertTrue(agent2.isRecoveryNeeded());
        agent2.recover(1);
        ds2.runBackgroundOperations();
        assertNotExists(ds2, TEST_PATH);
    }

    @Test
    public void recoveryWithSweepNodeChanged() throws Exception {
        // create some test data
        NodeBuilder builder = ds2.getRoot().builder();
        getOrCreate(builder, TEST_PATH);
        merge(ds2, builder);
        ds2.runBackgroundOperations();
        ds1.runBackgroundOperations();
        // now these nodes are visible on ds1
        assertExists(ds1, TEST_PATH);
        // wait a bit
        clock.waitUntil(clock.getTime() + SECONDS.toMillis(10));
        // set a property
        builder = ds1.getRoot().builder();
        getOrCreate(builder, TEST_PATH).setProperty("p", "v");
        // modify something on the remaining parent to move the commit root there
        getOrCreate(builder, BASE_PATH).setProperty("p", "v");
        merge(ds1, builder);
        // simulate a crash
        fds1.fail().after(0).eternally();
        disposeQuietly(ds1);
        // wait and run recovery for ds1
        clock.waitUntil(clock.getTime() + MINUTES.toMillis(3));
        ds2.renewClusterIdLease();

        assertTrue(agent2.isRecoveryNeeded());
        agent2.recover(1);
        ds2.runBackgroundOperations();
        assertPropertyExists(ds2, new Path(TEST_PATH, "p"));
    }

    private void assertPropertyExists(NodeStore ns, Path path) {
        NodeState state = ns.getRoot();
        Path parent = path.getParent();
        assertNotNull(parent);
        for (String name : parent.elements()) {
            state = state.getChildNode(name);
            assertTrue(state.exists());
        }
        assertTrue(state.hasProperty(path.getName()));
    }

    private void assertExists(NodeStore ns, Path path) {
        NodeState state = ns.getRoot();
        for (String name : path.elements()) {
            state = state.getChildNode(name);
            assertTrue(state.exists());
        }
    }

    private void assertNotExists(NodeStore ns, Path path) {
        NodeState state = ns.getRoot();
        for (String name : path.elements()) {
            state = state.getChildNode(name);
        }
        assertFalse(state.exists());
    }

    private NodeBuilder getOrCreate(NodeBuilder builder, Path path) {
        for (String name : path.elements()) {
            builder = builder.child(name);
        }
        return builder;
    }

}
