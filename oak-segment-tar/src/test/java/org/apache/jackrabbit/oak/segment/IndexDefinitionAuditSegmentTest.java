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
package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit;
import org.apache.jackrabbit.oak.plugins.index.IndexManagerMBeanImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Index definition audit and Index Manager on the segment node store (TarMK).
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12458">OAK-12458</a>
 */
public class IndexDefinitionAuditSegmentTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private String auditProperty;

    private EditorHook hook;

    @Before
    public void enableAudit() {
        auditProperty = System.getProperty(IndexDefinitionAudit.ENABLED_PROPERTY);
        System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, "true");
        hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
    }

    @After
    public void restoreAudit() {
        if (auditProperty == null) {
            System.clearProperty(IndexDefinitionAudit.ENABLED_PROPERTY);
        } else {
            System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, auditProperty);
        }
    }

    @Test
    public void indexManagerAndAuditTrailAfterRestart() throws Exception {
        try (FileStore fileStore = fileStoreBuilder(folder.getRoot()).build()) {
            SegmentNodeStore store = SegmentNodeStoreBuilders.builder(fileStore).build();
            initialize(store);
            NodeBuilder builder = store.getRoot().builder();
            NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "fooIndex", true, false,
                    Set.of("foo"), null);
            index.setProperty(ASYNC_PROPERTY_NAME, "async");
            store.merge(builder, hook, new CommitInfo("session-1", "alice"));
            builder = store.getRoot().builder();
            NodeBuilder data = builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").child(":data");
            for (int i = 0; i < 2500; i++) {
                data.child("n" + i).setProperty("x", i);
            }
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            IndexManagerMBeanImpl mbean = new IndexManagerMBeanImpl(store, new PropertyIndexEditorProvider(), null);
            mbean.disableIndex("fooIndex", "bob", "old version", false);
            mbean.enableIndex("fooIndex", "bob", false);
            mbean.disableIndex("fooIndex", "bob", null, false);
            String result = mbean.deleteIndex("fooIndex", "bob", "replaced", false);

            assertTrue(result, result.matches("Deleted /oak:index/fooIndex \\(25\\d\\d index data nodes removed in [2-9] commits\\)"));
            assertFalse(store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode("fooIndex"));
            fileStore.flush();
        }
        // the audit trail is persisted
        try (FileStore fileStore = fileStoreBuilder(folder.getRoot()).build()) {
            SegmentNodeStore store = SegmentNodeStoreBuilders.builder(fileStore).build();
            IndexManagerMBeanImpl mbean = new IndexManagerMBeanImpl(store, new PropertyIndexEditorProvider(), null);
            List<String> operations = new ArrayList<>();
            List<String> reasons = new ArrayList<>();
            for (Object row : mbean.getAuditTrail("fooIndex").values()) {
                operations.add((String) ((CompositeData) row).get("1. operation"));
                reasons.add((String) ((CompositeData) row).get("4. reason"));
            }
            assertEquals(List.of("DELETED", "DISABLED", "ENABLED", "DISABLED", "CREATED"), operations);
            assertEquals(List.of("replaced", "", "", "old version", ""), reasons);
        }
    }

    @Test
    public void changesBasedOnTheSameHeadAreBothRecorded() throws Exception {
        try (FileStore fileStore = fileStoreBuilder(folder.getRoot()).build()) {
            SegmentNodeStore store = SegmentNodeStoreBuilders.builder(fileStore).build();
            initialize(store);
            NodeBuilder builder = store.getRoot().builder();
            createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "fooIndex", true, false, Set.of("foo"), null);
            store.merge(builder, hook, new CommitInfo("session-1", "alice"));

            NodeBuilder builder1 = store.getRoot().builder();
            NodeBuilder builder2 = store.getRoot().builder();
            builder1.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").setProperty("a", 1L);
            builder2.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").setProperty("b", 2L);
            store.merge(builder1, hook, new CommitInfo("session-1", "alice"));
            // rebased on the first change: the commit hooks run again
            store.merge(builder2, hook, new CommitInfo("session-2", "bob"));

            NodeState history = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME)
                    .getChildNode(":audit").getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
            List<String> users = new ArrayList<>();
            for (ChildNodeEntry entry : history.getChildNodeEntries()) {
                users.add(entry.getNodeState().getString("operation") + ":" + entry.getNodeState().getString("user"));
            }
            assertEquals(3, users.size());
            assertEquals(Set.of("CREATED:alice", "CHANGED:alice", "CHANGED:bob"), Set.copyOf(users));
        }
    }

    private static void initialize(SegmentNodeStore store) throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        InitialContentHelper.INITIAL_CONTENT.compareAgainstBaseState(store.getRoot(),
                new ApplyDiff(builder));
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
