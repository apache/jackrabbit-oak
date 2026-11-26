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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ORIGINAL_TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DISABLED_BY;
import static org.apache.jackrabbit.oak.plugins.index.IndexManager.DISABLED_AT;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DISABLED_REASON;
import static org.apache.jackrabbit.oak.plugins.index.IndexManager.ORIGINAL_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12458">OAK-12458</a>
 */
public class IndexManagerMBeanImplTest {

    private static final CommitInfo ALICE = new CommitInfo("session-1", "alice");

    private static final IndexEditorProvider EDITORS = new PropertyIndexEditorProvider();

    private MemoryNodeStore store;

    private EditorHook hook;

    private IndexManagerMBeanImpl mbean;

    private Feature feature;

    private FeatureToggle toggle;

    private String auditProperty;

    @Before
    public void setUp() throws Exception {
        auditProperty = System.getProperty(IndexDefinitionAudit.ENABLED_PROPERTY);
        System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, "true");
        try {
            hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
        } finally {
            System.clearProperty(IndexDefinitionAudit.ENABLED_PROPERTY);
        }
        store = new MemoryNodeStore(INITIAL_CONTENT);
        Whiteboard whiteboard = new DefaultWhiteboard();
        feature = Feature.newFeature(IndexUpdateProvider.FT_OAK_12458, whiteboard);
        toggle = WhiteboardUtils.getServices(whiteboard, FeatureToggle.class).get(0);
        toggle.setEnabled(true);
        mbean = new IndexManagerMBeanImpl(store, EDITORS, feature);
    }

    @After
    public void tearDown() {
        if (auditProperty == null) {
            System.clearProperty(IndexDefinitionAudit.ENABLED_PROPERTY);
        } else {
            System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, auditProperty);
        }
        feature.close();
    }

    @Test
    public void noAudit() throws Exception {
        assertTrue(mbean.getIndexes().isEmpty());
        assertTrue(mbean.getAuditTrail("/oak:index/fooIndex").isEmpty());
        assertTrue(mbean.getAuditTrail(null).isEmpty());
    }

    @Test
    public void indexes() throws Exception {
        addIndex("/content", "barIndex");
        addIndex("/", "fooIndex");
        addIndex("/", "removedIndex");
        removeIndex("removedIndex");

        List<CompositeData> rows = rows(mbean.getIndexes());
        assertEquals(List.of("/content/oak:index/barIndex", "/oak:index/fooIndex", "/oak:index/removedIndex"),
                column(rows, "1. index"));
        CompositeData foo = rows.get(1);
        assertEquals(1, foo.get("4. entries"));
        assertEquals("CREATED", foo.get("2. lastOperation"));
        assertEquals("alice", foo.get("5. lastUser"));
        assertFalse(((String) foo.get("3. lastTime")).isEmpty());
        CompositeData removed = rows.get(2);
        assertEquals(Set.of("1. index", "2. lastOperation", "3. lastTime", "4. entries", "5. lastUser"),
                removed.getCompositeType().keySet());
        assertEquals(2, removed.get("4. entries"));
        assertEquals("DELETED", removed.get("2. lastOperation"));
        // only the entries of the given index
        assertEquals(List.of("CREATED"), column(rows(mbean.getAuditTrail("fooIndex")), "1. operation"));
    }

    @Test
    public void auditTrailNewestFirst() throws Exception {
        addIndex("/", "fooIndex");
        changeIndex("fooIndex", "type", "disabled");

        List<CompositeData> rows = rows(mbean.getAuditTrail("/oak:index/fooIndex"));
        assertEquals(List.of("DISABLED", "CREATED"), column(rows, "1. operation"));
        CompositeData added = rows.get(1);
        assertEquals(Set.of("1. operation", "2. time", "3. user", "4. reason", "5. thread", "6. diff"),
                added.getCompositeType().keySet());
        assertEquals("alice", added.get("3. user"));
        assertEquals("", added.get("4. reason"));
        assertEquals(Thread.currentThread().getName(), added.get("5. thread"));
        assertTrue((String) added.get("6. diff"), ((String) added.get("6. diff")).contains("propertyNames"));
        // the index name alone is resolved below /oak:index
        assertEquals(2, mbean.getAuditTrail(" fooIndex ").size());
    }

    @Test
    public void auditTrailOfRemovedIndex() throws Exception {
        addIndex("/", "fooIndex");
        removeIndex("fooIndex");

        List<CompositeData> rows = rows(mbean.getAuditTrail("/oak:index/fooIndex"));
        assertEquals(List.of("DELETED", "CREATED"), column(rows, "1. operation"));
    }

    @Test
    public void changesRequireTheAudit() throws Exception {
        addIndex("/", "fooIndex");
        toggle.setEnabled(false);

        assertThrows(IllegalStateException.class, () -> mbean.disableIndex("fooIndex", "bob", null, false));
        assertThrows(IllegalStateException.class, () -> mbean.enableIndex("fooIndex", "bob", false));
        assertThrows(IllegalStateException.class, () -> mbean.deleteIndex("fooIndex", "bob", null, false));
        assertEquals("property", fooIndex().getString(TYPE_PROPERTY_NAME));
        assertThrows(IllegalStateException.class,
                () -> new IndexManagerMBeanImpl(store, EDITORS, null).disableIndex("fooIndex", "bob", null, false));

        // the system property also enables it, e.g. in oak-run
        System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, "true");
        assertEquals("Disabled /oak:index/fooIndex",
                new IndexManagerMBeanImpl(store, EDITORS, null).disableIndex("fooIndex", "bob", null, false));
    }

    @Test
    public void systemPropertySetAfterStartDoesNotAllowChanges() throws Exception {
        addIndex("/", "fooIndex");
        IndexManagerMBeanImpl withoutToggle = new IndexManagerMBeanImpl(store, EDITORS, null);
        // its own commits would not be recorded
        System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, "true");

        assertThrows(IllegalStateException.class, () -> withoutToggle.disableIndex("fooIndex", "bob", null, false));
    }

    @Test
    public void disable() throws Exception {
        addIndex("/", "fooIndex");
        changeIndex("fooIndex", DISABLED_REASON, "left over from an earlier disable");

        assertEquals("Disabled /oak:index/fooIndex", mbean.disableIndex(" fooIndex ", " sky-idx:bob ", " old version ", false));
        NodeState index = fooIndex();
        assertEquals(TYPE_DISABLED, index.getString(TYPE_PROPERTY_NAME));
        assertEquals("property", index.getString(ORIGINAL_TYPE));
        assertEquals("sky-idx:bob", index.getString(DISABLED_BY));
        assertEquals("old version", index.getString(DISABLED_REASON));
        Instant.parse(index.getString(DISABLED_AT));
        CompositeData entry = rows(mbean.getAuditTrail("fooIndex")).get(0);
        assertEquals("DISABLED", entry.get("1. operation"));
        assertEquals("sky-idx:bob", entry.get("3. user"));
        assertEquals("old version", entry.get("4. reason"));
        assertEquals("IndexManager.disableIndex", entry.get("5. thread"));
        assertEquals(IndexManager.ORIGIN, lastAuditEntry("fooIndex").getString(IndexDefinitionAudit.PROP_ORIGIN));

        assertEquals("/oak:index/fooIndex is already disabled", mbean.disableIndex("fooIndex", "bob", null, false));
        assertEquals(3, mbean.getAuditTrail("fooIndex").size());
    }

    @Test
    public void disableWithoutCallerAndReason() throws Exception {
        addIndex("/", "fooIndex");
        changeIndex("fooIndex", DISABLED_REASON, "left over from an earlier disable");

        mbean.disableIndex("/oak:index/fooIndex", null, " ", false);

        assertFalse(fooIndex().hasProperty(DISABLED_REASON));
        assertEquals(IndexManagerMBeanImpl.UNKNOWN_CALLER, fooIndex().getString(DISABLED_BY));
        assertEquals(IndexManagerMBeanImpl.UNKNOWN_CALLER, rows(mbean.getAuditTrail("fooIndex")).get(0).get("3. user"));
    }

    @Test
    public void enable() throws Exception {
        addIndex("/", "fooIndex");
        assertEquals("/oak:index/fooIndex is not disabled", mbean.enableIndex("fooIndex", "bob", false));
        assertEquals(1, mbean.getAuditTrail("fooIndex").size());
        // as if built by the async lane
        NodeBuilder builder = store.getRoot().builder();
        builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex")
                .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, false);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        mbean.disableIndex("fooIndex", "bob", "old version", false);

        assertEquals("Enabled /oak:index/fooIndex (type property), reindexing", mbean.enableIndex("fooIndex", "alice", false));
        NodeState index = fooIndex();
        assertEquals("property", index.getString(TYPE_PROPERTY_NAME));
        // the index was not updated while disabled
        assertTrue(index.getBoolean(IndexConstants.REINDEX_PROPERTY_NAME));
        assertTrue(lastAuditEntry("fooIndex").getBoolean(IndexDefinitionAudit.PROP_REINDEX_REQUESTED));
        for (String name : List.of(ORIGINAL_TYPE, DISABLED_AT, DISABLED_BY, DISABLED_REASON)) {
            assertFalse(name, index.hasProperty(name));
        }
        CompositeData entry = rows(mbean.getAuditTrail("fooIndex")).get(0);
        assertEquals("ENABLED", entry.get("1. operation"));
        assertEquals("alice", entry.get("3. user"));
        assertEquals("IndexManager.enableIndex", entry.get("5. thread"));
    }

    @Test
    public void enableIndexDisabledByPurge() throws Exception {
        addIndex("/", "fooIndex");
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder index = builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
        index.setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED);
        index.setProperty(ORIGINAL_TYPE_PROPERTY_NAME, "property");
        store.merge(builder, hook, ALICE);

        mbean.enableIndex("fooIndex", "bob", false);

        assertEquals("property", fooIndex().getString(TYPE_PROPERTY_NAME));
        assertFalse(fooIndex().hasProperty(ORIGINAL_TYPE_PROPERTY_NAME));
    }

    @Test
    public void enableWithUnknownType() throws Exception {
        addIndex("/", "fooIndex");
        changeIndex("fooIndex", TYPE_PROPERTY_NAME, TYPE_DISABLED);

        assertThrows(IllegalStateException.class, () -> mbean.enableIndex("fooIndex", "bob", false));
        changeIndex("fooIndex", ORIGINAL_TYPE, " ");
        assertThrows(IllegalStateException.class, () -> mbean.enableIndex("fooIndex", "bob", false));
        assertEquals(TYPE_DISABLED, fooIndex().getString(TYPE_PROPERTY_NAME));
    }

    @Test
    public void deleteRequiresDisabledIndex() throws Exception {
        addIndex("/", "fooIndex");

        assertThrows(IllegalStateException.class, () -> mbean.deleteIndex("fooIndex", "bob", null, false));
        assertTrue(fooIndex().exists());
    }

    @Test
    public void deleteLargeIndex() throws Exception {
        addIndex("/", "fooIndex");
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder data = builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").child(":data");
        for (int i = 0; i < 2500; i++) {
            data.child("n" + i).setProperty("x", i);
        }
        // part of the definition, removed with it
        builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").child("rule").setProperty("x", 1L);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        mbean.disableIndex("fooIndex", "bob", "old version", false);

        String result = mbean.deleteIndex("fooIndex", "sky-idx-svc", " replaced by fooIndex-2 ", false);

        // the index data is deleted in batches
        assertTrue(result, result.matches("Deleted /oak:index/fooIndex \\(25\\d\\d index data nodes removed in [2-9] commits\\)"));
        assertFalse(fooIndex().exists());
        List<CompositeData> trail = rows(mbean.getAuditTrail("fooIndex"));
        assertEquals(List.of("DELETED", "DISABLED", "CREATED"), column(trail, "1. operation"));
        assertEquals("sky-idx-svc", trail.get(0).get("3. user"));
        assertEquals("replaced by fooIndex-2", trail.get(0).get("4. reason"));
        assertEquals("IndexManager.deleteIndex", trail.get(0).get("5. thread"));
        assertEquals("old version", trail.get(1).get("4. reason"));
        assertEquals("", trail.get(0).get("6. diff"));
    }

    @Test
    public void deleteIndexWithoutData() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.getChildNode(INDEX_DEFINITIONS_NAME).child("fooIndex").setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED)
                .setProperty(IndexConstants.ASYNC_PROPERTY_NAME, "async");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals("Deleted /oak:index/fooIndex (0 index data nodes removed in 0 commits)",
                mbean.deleteIndex("fooIndex", "bob", null, false));
        assertFalse(fooIndex().exists());
    }

    @Test
    public void nestedIndexCanNotBeChanged() throws Exception {
        addIndex("/content", "barIndex");

        String path = "/content/oak:index/barIndex";
        assertThrows(IllegalArgumentException.class, () -> mbean.disableIndex(path, "bob", null, false));
        assertThrows(IllegalArgumentException.class, () -> mbean.enableIndex(path, "bob", false));
        assertThrows(IllegalArgumentException.class, () -> mbean.deleteIndex(path, "bob", null, false));
        assertTrue(NodeStateUtils.getNode(store.getRoot(), path).exists());
        // its history is recorded and can be read
        assertEquals(List.of("CREATED"), column(rows(mbean.getAuditTrail(path)), "1. operation"));
    }

    @Test
    public void notAnIndexDefinition() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("content").child("typed").setProperty(TYPE_PROPERTY_NAME, "property");
        builder.getChildNode(INDEX_DEFINITIONS_NAME).child("untyped");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        for (String path : new String[] {null, "/", "/oak:index", "missing", "untyped", "/content/typed",
                "uuid/", "/oak:index//uuid", "../oak:index/uuid", "x/../uuid", "/oak:index/../oak:index/uuid"}) {
            assertThrows(path, IllegalArgumentException.class, () -> mbean.disableIndex(path, "bob", null, false));
            assertThrows(path, IllegalArgumentException.class, () -> mbean.enableIndex(path, "bob", false));
            assertThrows(path, IllegalArgumentException.class, () -> mbean.deleteIndex(path, "bob", null, false));
        }
    }

    @Test
    public void synchronousIndexRequiresForce() throws Exception {
        assertThrows(IllegalStateException.class, () -> mbean.disableIndex("uuid", "bob", null, false));
        assertEquals("property", store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("uuid")
                .getString(TYPE_PROPERTY_NAME));

        addSynchronousIndex("syncIndex");
        assertEquals("Disabled /oak:index/syncIndex", mbean.disableIndex("syncIndex", "bob", null, true));
        assertThrows(IllegalStateException.class, () -> mbean.deleteIndex("syncIndex", "bob", null, false));
        mbean.deleteIndex("syncIndex", "bob", null, true);
        assertFalse(store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode("syncIndex"));
    }

    @Test
    public void deleteUpdatesTheNodeTypeIndex() throws Exception {
        addIndex("/", "fooIndex");
        assertTrue(isInNodeTypeIndex("/oak:index/fooIndex"));
        mbean.disableIndex("fooIndex", "bob", null, false);

        mbean.deleteIndex("fooIndex", "bob", null, false);

        assertFalse(isInNodeTypeIndex("/oak:index/fooIndex"));
    }

    @Test
    public void enableReindexesSynchronousIndexInTheSameCommit() throws Exception {
        addSynchronousIndex("syncIndex");
        addContent("a", "x");
        mbean.disableIndex("syncIndex", "bob", null, true);
        // not indexed while disabled
        addContent("b", "y");
        NodeState index = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("syncIndex");
        assertFalse(index.getChildNode(":index").hasChildNode("y"));

        // the reindex of a synchronous index runs in this call
        assertThrows(IllegalStateException.class, () -> mbean.enableIndex("syncIndex", "bob", false));
        mbean.enableIndex("syncIndex", "bob", true);

        index = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("syncIndex");
        assertFalse(index.getBoolean(IndexConstants.REINDEX_PROPERTY_NAME));
        assertTrue(index.getChildNode(":index").hasChildNode("x"));
        assertTrue(index.getChildNode(":index").hasChildNode("y"));
        CompositeData entry = rows(mbean.getAuditTrail("syncIndex")).get(0);
        assertEquals("ENABLED", entry.get("1. operation"));
        assertEquals("IndexManager.enableIndex", entry.get("5. thread"));
    }

    @Test
    public void deleteFailsIfEnabledWhileDeletingIndexData() throws Exception {
        assertDeleteFailsIfEnabledMeanwhile(true);
    }

    @Test
    public void deleteFailsIfEnabledBeforeRemovingTheDefinition() throws Exception {
        assertDeleteFailsIfEnabledMeanwhile(false);
    }

    private void assertDeleteFailsIfEnabledMeanwhile(boolean withIndexData) throws Exception {
        AtomicBoolean enableBeforeNextChange = new AtomicBoolean();
        store = new MemoryNodeStore(INITIAL_CONTENT) {
            @Override
            public synchronized NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
                    @NotNull CommitInfo info) throws CommitFailedException {
                if (IndexManager.ORIGIN.equals(info.getSessionId()) && enableBeforeNextChange.getAndSet(false)) {
                    // a concurrent change enables the index again
                    NodeBuilder other = getRoot().builder();
                    other.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").setProperty(TYPE_PROPERTY_NAME, "property");
                    super.merge(other, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }
                return super.merge(builder, commitHook, info);
            }
        };
        mbean = new IndexManagerMBeanImpl(store, EDITORS, feature);
        addIndex("/", "fooIndex");
        if (withIndexData) {
            NodeBuilder builder = store.getRoot().builder();
            builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").child(":data").child("n");
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        mbean.disableIndex("fooIndex", "bob", null, false);
        enableBeforeNextChange.set(true);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> mbean.deleteIndex("fooIndex", "bob", null, false));

        assertEquals("property", fooIndex().getString(TYPE_PROPERTY_NAME));
        assertEquals(withIndexData, fooIndex().getChildNode(":data").hasChildNode("n"));
        // nothing was removed
        assertFalse(e.getMessage(), e.getMessage().contains("partly removed"));
    }

    @Test
    public void indexCreatedAgainWhileTheAuditWasDisabled() throws Exception {
        addIndex("/", "fooIndex");
        mbean.disableIndex("fooIndex", "bob", null, false);
        mbean.deleteIndex("fooIndex", "bob", null, false);
        // created again without the audit: the history stays with the removed indexes
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "fooIndex", true, false, Set.of("foo"), null)
                .setProperty(IndexConstants.ASYNC_PROPERTY_NAME, "async");
        store.merge(builder, new EditorHook(new IndexUpdateProvider(EDITORS)), ALICE);

        changeIndex("fooIndex", "info", "changed");

        // the history continues
        assertFalse(store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(IndexDefinitionAudit.AUDIT_NODE_NAME)
                .hasChildNode(IndexDefinitionAudit.DELETED_NODE_NAME));
        List<CompositeData> indexes = rows(mbean.getIndexes());
        assertEquals(1, indexes.size());
        assertEquals(4, indexes.get(0).get("4. entries"));
        assertEquals(List.of("CHANGED", "DELETED", "DISABLED", "CREATED"),
                column(rows(mbean.getAuditTrail("fooIndex")), "1. operation"));
    }

    @Test
    public void indexesWithHistoryAndHistoryOfRemovedIndex() throws Exception {
        addIndex("/", "fooIndex");
        mbean.disableIndex("fooIndex", "bob", null, false);
        mbean.deleteIndex("fooIndex", "bob", null, false);
        // a history next to the history of the removed index, with the same path
        NodeBuilder builder = store.getRoot().builder();
        builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(IndexDefinitionAudit.AUDIT_NODE_NAME)
                .child(INDEX_DEFINITIONS_NAME).child("fooIndex").child("29990101T0000Z.0")
                .setProperty(IndexDefinitionAudit.PROP_OPERATION, "CREATED");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        List<CompositeData> indexes = rows(mbean.getIndexes());

        assertEquals(1, indexes.size());
        assertEquals(4, indexes.get(0).get("4. entries"));
        assertEquals("CREATED", indexes.get(0).get("2. lastOperation"));
    }

    @Test
    public void callerFromJmxSubject() throws Exception {
        addIndex("/", "fooIndex");
        Subject subject = new Subject();
        subject.getPrincipals().add(new PrincipalImpl("jmx-admin"));

        Java23Subject.doAs(subject, (PrivilegedAction<String>) () -> mbean.disableIndex("fooIndex", " ", null, false));

        assertEquals("jmx-admin", fooIndex().getString(IndexDefinitionAudit.DISABLED_BY));
        assertEquals("jmx-admin", rows(mbean.getAuditTrail("fooIndex")).get(0).get("3. user"));

        // a JMX caller without principals is unknown
        Java23Subject.doAs(new Subject(), (PrivilegedAction<String>) () -> mbean.enableIndex("fooIndex", null, false));
        assertEquals(IndexManagerMBeanImpl.UNKNOWN_CALLER, rows(mbean.getAuditTrail("fooIndex")).get(0).get("3. user"));
    }

    @Test
    public void valuesAreLimited() throws Exception {
        addIndex("/", "fooIndex");
        String tooLong = "x".repeat(IndexDefinitionAudit.MAX_VALUE_LENGTH + 1);
        String longest = "x".repeat(IndexDefinitionAudit.MAX_VALUE_LENGTH);

        assertThrows(IllegalArgumentException.class, () -> mbean.disableIndex("fooIndex", tooLong, null, false));
        assertThrows(IllegalArgumentException.class, () -> mbean.disableIndex("fooIndex", "bob", tooLong, false));
        assertThrows(IllegalArgumentException.class, () -> mbean.enableIndex("fooIndex", tooLong, false));
        assertThrows(IllegalArgumentException.class, () -> mbean.deleteIndex("fooIndex", tooLong, null, false));
        assertThrows(IllegalArgumentException.class, () -> mbean.deleteIndex("fooIndex", "bob", tooLong, false));
        assertEquals("property", fooIndex().getString(TYPE_PROPERTY_NAME));
        assertEquals(1, mbean.getAuditTrail("fooIndex").size());

        mbean.disableIndex("fooIndex", longest, longest, false);
        CompositeData entry = rows(mbean.getAuditTrail("fooIndex")).get(0);
        assertEquals(longest, entry.get("3. user"));
        assertEquals(longest, entry.get("4. reason"));
    }

    @Test
    public void brokenAuditEntriesCanBeRead() throws Exception {
        addIndex("/", "fooIndex");
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder history = builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(IndexDefinitionAudit.AUDIT_NODE_NAME)
                .getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
        // edited by hand: a sequence number that does not fit into a long (not an entry), wrong types
        history.child("20260101T0000Z.0").setProperty(IndexDefinitionAudit.PROP_OPERATION, "CHANGED");
        history.child("20260101T0000Z.99999999999999999999").setProperty(IndexDefinitionAudit.PROP_OPERATION, "X");
        history.child("20260101T0000Z.1").setProperty(IndexDefinitionAudit.PROP_TIME, "yesterday")
                .setProperty(IndexDefinitionAudit.PROP_USER, 42L)
                .setProperty(IndexDefinitionAudit.PROP_OPERATION, List.of("A", "B"), org.apache.jackrabbit.oak.api.Type.STRINGS);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        List<CompositeData> trail = rows(mbean.getAuditTrail("fooIndex"));
        assertEquals(List.of("CREATED", "", "CHANGED"), column(trail, "1. operation"));
        assertEquals("yesterday", trail.get(1).get("2. time"));
        assertEquals("42", trail.get(1).get("3. user"));
        assertEquals(3, rows(mbean.getIndexes()).get(0).get("4. entries"));
        // the index can still be changed
        mbean.disableIndex("fooIndex", "bob", null, false);
        assertEquals(4, mbean.getAuditTrail("fooIndex").size());
    }

    @Test
    public void indexReplacedWhileItIsDeletedIsNotDeleted() throws Exception {
        AtomicBoolean replaceBeforeNextChange = new AtomicBoolean();
        store = new MemoryNodeStore(INITIAL_CONTENT) {
            @Override
            public synchronized NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
                    @NotNull CommitInfo info) throws CommitFailedException {
                if (IndexManager.ORIGIN.equals(info.getSessionId()) && replaceBeforeNextChange.getAndSet(false)) {
                    // a concurrent change replaces the definition (also disabled)
                    NodeBuilder other = getRoot().builder();
                    other.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").setProperty("propertyNames", "bar");
                    super.merge(other, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }
                return super.merge(builder, commitHook, info);
            }
        };
        mbean = new IndexManagerMBeanImpl(store, EDITORS, feature);
        addIndex("/", "fooIndex");
        mbean.disableIndex("fooIndex", "bob", null, false);
        replaceBeforeNextChange.set(true);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> mbean.deleteIndex("fooIndex", "bob", null, false));

        assertTrue(e.getMessage(), e.getMessage().contains("changed meanwhile"));
        assertEquals("bar", fooIndex().getString("propertyNames"));
        assertEquals(List.of("DISABLED", "CREATED"), column(rows(mbean.getAuditTrail("fooIndex")), "1. operation"));
    }

    @Test
    public void indexChangedOrNoLongerReindexedWhileItsDataIsDeletedIsNotDeleted() throws Exception {
        assertDeleteStopsAfterFirstBatch("propertyNames", "bar", "changed meanwhile");
        assertDeleteStopsAfterFirstBatch(IndexConstants.REINDEX_PROPERTY_NAME, false, "no longer marked for reindexing");
    }

    private void assertDeleteStopsAfterFirstBatch(String property, Object value, String message) throws Exception {
        AtomicInteger merges = new AtomicInteger();
        store = new MemoryNodeStore(INITIAL_CONTENT) {
            @Override
            public synchronized NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
                    @NotNull CommitInfo info) throws CommitFailedException {
                if (IndexManager.ORIGIN.equals(info.getSessionId()) && merges.incrementAndGet() == 4) {
                    // after the first batch of index data is deleted, the definition is changed
                    NodeBuilder other = getRoot().builder();
                    NodeBuilder index = other.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
                    if (value instanceof Boolean) {
                        index.setProperty(property, (Boolean) value);
                    } else {
                        index.setProperty(property, (String) value);
                    }
                    super.merge(other, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }
                return super.merge(builder, commitHook, info);
            }
        };
        mbean = new IndexManagerMBeanImpl(store, EDITORS, feature);
        addIndex("/", "fooIndex");
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder index = builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
        index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, false);
        for (int i = 0; i < 2500; i++) {
            index.child(":data").child("n" + i).setProperty("x", i);
        }
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        mbean.disableIndex("fooIndex", "bob", null, false);
        merges.set(1);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> mbean.deleteIndex("fooIndex", "bob", null, false));

        assertTrue(e.getMessage(), e.getMessage().contains(message));
        long remaining = fooIndex().getChildNode(":data").getChildNodeCount(10_000);
        assertTrue(String.valueOf(remaining), remaining > 0 && remaining < 2500);
        assertEquals(List.of("DISABLED", "CREATED"), column(rows(mbean.getAuditTrail("fooIndex")), "1. operation"));
    }

    @Test
    public void indexChangedBeforeItsDefinitionIsRemovedIsNotDeleted() throws Exception {
        store = new MemoryNodeStore(INITIAL_CONTENT) {
            @Override
            public synchronized NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
                    @NotNull CommitInfo info) throws CommitFailedException {
                if ("IndexManager.deleteIndex".equals(info.getInfo().get(IndexDefinitionAudit.COMMIT_INFO_THREAD))) {
                    // after all index data is deleted, the reindex flag is reset
                    NodeBuilder other = getRoot().builder();
                    other.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex")
                            .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, false);
                    super.merge(other, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }
                return super.merge(builder, commitHook, info);
            }
        };
        mbean = new IndexManagerMBeanImpl(store, EDITORS, feature);
        addIndex("/", "fooIndex");
        NodeBuilder builder = store.getRoot().builder();
        builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").child(":data").child("n");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        mbean.disableIndex("fooIndex", "bob", null, false);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> mbean.deleteIndex("fooIndex", "bob", null, false));

        assertTrue(e.getMessage(), e.getMessage().contains("no longer marked for reindexing"));
        assertTrue(e.getMessage(), e.getMessage().contains("index data was partly removed in 1 commits"));
        assertTrue(fooIndex().exists());
        assertFalse(fooIndex().hasChildNode(":data"));
    }

    @Test
    public void disableAndEnableFailIfTheIndexIsChangedMeanwhile() throws Exception {
        AtomicBoolean changeBeforeNextChange = new AtomicBoolean();
        AtomicInteger changes = new AtomicInteger();
        AtomicReference<BiConsumer<NodeBuilder, Integer>> concurrentChange = new AtomicReference<>(
                (index, n) -> index.setProperty("info", "change " + n));
        store = new MemoryNodeStore(INITIAL_CONTENT) {
            @Override
            public synchronized NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
                    @NotNull CommitInfo info) throws CommitFailedException {
                if (IndexManager.ORIGIN.equals(info.getSessionId()) && changeBeforeNextChange.getAndSet(false)) {
                    // a concurrent change of the definition, between reading it and the commit
                    NodeBuilder other = getRoot().builder();
                    NodeBuilder index = other.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
                    concurrentChange.get().accept(index, changes.incrementAndGet());
                    super.merge(other, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }
                return super.merge(builder, commitHook, info);
            }
        };
        mbean = new IndexManagerMBeanImpl(store, EDITORS, feature);
        addIndex("/", "fooIndex");

        changeBeforeNextChange.set(true);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> mbean.disableIndex("fooIndex", "bob", null, false));
        assertTrue(e.getMessage(), e.getMessage().contains("was changed meanwhile, it is not disabled"));
        assertEquals("property", fooIndex().getString(TYPE_PROPERTY_NAME));

        mbean.disableIndex("fooIndex", "bob", null, false);
        changeBeforeNextChange.set(true);
        e = assertThrows(IllegalStateException.class, () -> mbean.enableIndex("fooIndex", "bob", false));
        assertTrue(e.getMessage(), e.getMessage().contains("was changed meanwhile, it is not enabled"));
        assertEquals(TYPE_DISABLED, fooIndex().getString(TYPE_PROPERTY_NAME));
        // the type set by the purge of old index versions is read by enable
        changeBeforeNextChange.set(true);
        concurrentChange.set((index, n) -> index.setProperty(ORIGINAL_TYPE_PROPERTY_NAME, "lucene"));
        e = assertThrows(IllegalStateException.class, () -> mbean.enableIndex("fooIndex", "bob", false));
        assertTrue(e.getMessage(), e.getMessage().contains("was changed meanwhile, it is not enabled"));
        // a reindex (by the indexing lane) meanwhile does not matter
        changeBeforeNextChange.set(true);
        concurrentChange.set((index, n) -> index.setProperty(IndexConstants.REINDEX_COUNT, (long) n));
        mbean.enableIndex("fooIndex", "bob", false);
        assertEquals("property", fooIndex().getString(TYPE_PROPERTY_NAME));
        // the concurrent changes are not recorded (committed without the index update)
        assertEquals(List.of("ENABLED", "DISABLED", "CREATED"), column(rows(mbean.getAuditTrail("fooIndex")), "1. operation"));
    }

    @Test
    public void identicalEntriesAreShownOnce() throws Exception {
        addIndex("/", "fooIndex");
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder history = builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(IndexDefinitionAudit.AUDIT_NODE_NAME)
                .getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
        // for example copied when histories are merged
        for (String name : List.of("20260101T0000Z.0", "20260101T0000Z.1")) {
            history.child(name).setProperty(IndexDefinitionAudit.PROP_OPERATION, "CHANGED")
                    .setProperty(IndexDefinitionAudit.PROP_USER, "bob");
        }
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals(List.of("CREATED", "CHANGED"), column(rows(mbean.getAuditTrail("fooIndex")), "1. operation"));
        assertEquals(3, rows(mbean.getIndexes()).get(0).get("4. entries"));
    }

    @Test
    public void indexEnabledOutsideWhileItsDataIsDeletedIsReindexed() throws Exception {
        AtomicInteger merges = new AtomicInteger();
        store = new MemoryNodeStore(INITIAL_CONTENT) {
            @Override
            public synchronized NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
                    @NotNull CommitInfo info) throws CommitFailedException {
                if (IndexManager.ORIGIN.equals(info.getSessionId()) && merges.incrementAndGet() == 4) {
                    // after the first batch of index data is deleted, the index is enabled again
                    // without the index manager (for example by deploying the definition)
                    NodeBuilder other = getRoot().builder();
                    other.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").setProperty(TYPE_PROPERTY_NAME, "property");
                    super.merge(other, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }
                return super.merge(builder, commitHook, info);
            }
        };
        mbean = new IndexManagerMBeanImpl(store, EDITORS, feature);
        addIndex("/", "fooIndex");
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder index = builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
        // as if built by the async lane
        index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, false);
        for (int i = 0; i < 2500; i++) {
            index.child(":data").child("n" + i).setProperty("x", i);
        }
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // merge 1
        mbean.disableIndex("fooIndex", "bob", null, false);
        merges.set(1);

        // merge 2 sets the reindex flag, merge 3 is the first batch, the index is enabled before merge 4
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> mbean.deleteIndex("fooIndex", "bob", null, false));
        assertTrue(e.getMessage(), e.getMessage().contains("index data was partly removed in 1 commits: if the index is used, it must be reindexed"));

        NodeState foo = fooIndex();
        assertEquals("property", foo.getString(TYPE_PROPERTY_NAME));
        long remaining = foo.getChildNode(":data").getChildNodeCount(10_000);
        assertTrue(String.valueOf(remaining), remaining > 0 && remaining < 2500);
        // the partly deleted index is rebuilt
        assertTrue(foo.getBoolean(IndexConstants.REINDEX_PROPERTY_NAME));
    }

    @Test
    public void registeredAsStandardMBean() throws Exception {
        addIndex("/", "fooIndex");
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("org.apache.jackrabbit.oak:type=" + IndexManagerMBean.TYPE
                + ",name=test");
        server.registerMBean(mbean, name);
        try {
            assertEquals(1, ((TabularData) server.getAttribute(name, "Indexes")).size());
            TabularData trail = (TabularData) server.invoke(name, "getAuditTrail",
                    new Object[] {"fooIndex"}, new String[] {String.class.getName()});
            assertEquals(1, trail.size());
            assertEquals("Disabled /oak:index/fooIndex", server.invoke(name, "disableIndex",
                    new Object[] {"fooIndex", "bob", "test", false},
                    new String[] {String.class.getName(), String.class.getName(), String.class.getName(),
                            boolean.class.getName()}));
        } finally {
            server.unregisterMBean(name);
        }
    }

    private NodeState fooIndex() {
        return store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
    }

    private NodeState lastAuditEntry(String name) {
        NodeState history = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME)
                .getChildNode(IndexDefinitionAudit.AUDIT_NODE_NAME).getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(name);
        String last = null;
        for (String entry : history.getChildNodeNames()) {
            if (last == null || IndexDefinitionAudit.ENTRY_ORDER.compare(entry, last) > 0) {
                last = entry;
            }
        }
        return history.getChildNode(last);
    }

    private void addIndex(String parent, String name) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder definitions = builder;
        for (String element : PathUtils.elements(parent)) {
            definitions = definitions.child(element);
        }
        // an asynchronous index: synchronous indexes are only changed if forced
        createIndexDefinition(definitions.child(INDEX_DEFINITIONS_NAME), name, true, false, Set.of("foo"), null)
                .setProperty(IndexConstants.ASYNC_PROPERTY_NAME, "async");
        store.merge(builder, hook, ALICE);
    }

    private void addSynchronousIndex(String name) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), name, true, false, Set.of("foo"), null);
        store.merge(builder, hook, ALICE);
    }

    private void addContent(String name, String foo) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("content").child(name).setProperty("foo", foo);
        store.merge(builder, hook, ALICE);
    }

    /**
     * Whether the node type index (content mirror strategy) has the entry for the given index definition.
     */
    private boolean isInNodeTypeIndex(String indexPath) {
        NodeState entry = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("nodetype")
                .getChildNode(":index").getChildNode("oak%3AQueryIndexDefinition");
        for (String element : PathUtils.elements(indexPath)) {
            entry = entry.getChildNode(element);
        }
        return entry.getBoolean("match");
    }

    private void changeIndex(String name, String property, String value) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(name).setProperty(property, value);
        store.merge(builder, hook, ALICE);
    }

    private void removeIndex(String name) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(name).remove();
        store.merge(builder, hook, ALICE);
    }

    private static List<CompositeData> rows(TabularData data) {
        List<CompositeData> rows = new ArrayList<>();
        for (Object row : data.values()) {
            rows.add((CompositeData) row);
        }
        return rows;
    }

    private static List<Object> column(List<CompositeData> rows, String name) {
        List<Object> values = new ArrayList<>();
        for (CompositeData row : rows) {
            values.add(row.get(name));
        }
        return values;
    }
}
