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

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Index definition audit on a cluster: concurrent changes of the same index on two
 * cluster nodes within the same minute create the same audit entry name. The second
 * commit conflicts, is retried on the new head, and gets the next sequence number.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12458">OAK-12458</a>
 */
@RunWith(Parameterized.class)
public class IndexDefinitionAuditClusterTest {

    private final DocumentStoreFixture fixture;

    private DocumentStore store1;
    private DocumentStore store2;
    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;
    private EditorHook hook;

    public IndexDefinitionAuditClusterTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> fixtures() {
        List<Object[]> fixtures = new ArrayList<>();
        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});
        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (mongo.isAvailable()) {
            fixtures.add(new Object[] {mongo});
        }
        return fixtures;
    }

    @Before
    public void setUp() {
        MongoUtils.dropCollections(MongoUtils.DB);
        store1 = fixture.createDocumentStore(1);
        store2 = fixture.hasSinglePersistence() ? fixture.createDocumentStore(2) : store1;
        // background operations run in their own threads, so that a conflicting
        // commit can wait until the other cluster node's change is visible
        ns1 = new DocumentMK.Builder().setDocumentStore(store1).setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setClusterId(1).getNodeStore();
        ns2 = new DocumentMK.Builder().setDocumentStore(store2).setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setClusterId(2).getNodeStore();
        System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, "true");
        try {
            hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
        } finally {
            System.clearProperty(IndexDefinitionAudit.ENABLED_PROPERTY);
        }
    }

    @After
    public void tearDown() throws Exception {
        ns2.dispose();
        ns1.dispose();
        store2.dispose();
        store1.dispose();
        fixture.dispose();
    }

    @Test
    public void concurrentChangesOnTwoClusterNodes() throws Exception {
        NodeBuilder builder = ns1.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "fooIndex", true, false, Set.of("foo"), null);
        ns1.merge(builder, hook, new CommitInfo("session-1", "alice"));
        waitUntilVisible(ns -> fooIndex(ns).exists());

        // both cluster nodes change the index based on the same head
        NodeBuilder builder1 = ns1.getRoot().builder();
        NodeBuilder builder2 = ns2.getRoot().builder();
        fooIndex(builder1).setProperty("a", 1L);
        fooIndex(builder2).setProperty("b", 2L);
        ns1.merge(builder1, hook, new CommitInfo("session-1", "alice"));
        ns2.merge(builder2, hook, new CommitInfo("session-2", "bob"));
        waitUntilVisible(ns -> fooIndex(ns).hasProperty("a") && fooIndex(ns).hasProperty("b"));

        for (DocumentNodeStore ns : List.of(ns1, ns2)) {
            NodeState history = ns.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(":audit")
                    .getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
            List<String> operations = new ArrayList<>();
            List<String> users = new ArrayList<>();
            for (ChildNodeEntry entry : history.getChildNodeEntries()) {
                operations.add(entry.getNodeState().getString("operation"));
                users.add(entry.getNodeState().getString("user"));
            }
            // no entry is lost, and the retried commit does not write a duplicate
            assertEquals(3, operations.size());
            assertEquals(1, operations.stream().filter("CREATED"::equals).count());
            assertEquals(2, operations.stream().filter("CHANGED"::equals).count());
            assertTrue(users.toString(), users.contains("bob"));
            assertEquals(1L, fooIndex(ns).getLong("a"));
            assertEquals(2L, fooIndex(ns).getLong("b"));
        }
    }

    /**
     * Waits until the changes are visible on both cluster nodes, which happens in the
     * background threads of the cluster nodes.
     */
    private void waitUntilVisible(Predicate<DocumentNodeStore> condition) throws InterruptedException {
        long timeout = System.currentTimeMillis() + 10_000;
        while (!(condition.test(ns1) && condition.test(ns2))) {
            assertTrue("changes not visible on both cluster nodes", System.currentTimeMillis() < timeout);
            Thread.sleep(50);
        }
    }

    private static NodeState fooIndex(DocumentNodeStore ns) {
        return ns.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
    }

    private static NodeBuilder fooIndex(NodeBuilder builder) {
        return builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
    }
}
