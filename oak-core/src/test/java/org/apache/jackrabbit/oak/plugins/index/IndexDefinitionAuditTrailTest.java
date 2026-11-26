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

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.AUDIT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DELETED_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_DIFF;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_OPERATION;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_REASON;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_THREAD;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_TIME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_USER;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAuditTrail.Entry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12458">OAK-12458</a>
 */
public class IndexDefinitionAuditTrailTest {

    private final NodeBuilder root = EMPTY_NODE.builder();

    private final NodeBuilder audit = root.child(INDEX_DEFINITIONS_NAME).child(AUDIT_NODE_NAME);

    @Test
    public void noAuditTrail() {
        assertTrue(trail().getEntriesByIndex().isEmpty());
        assertTrue(trail().getEntries("/oak:index/fooIndex").isEmpty());
    }

    @Test
    public void invalidPaths() {
        entry(audit.child(INDEX_DEFINITIONS_NAME).child("fooIndex"), "20261013T1405Z.0", "CREATED");
        audit.child(DELETED_NODE_NAME).child(INDEX_DEFINITIONS_NAME).child("barIndex").child("20261013T1405Z.0");
        for (String path : List.of("", "/", "fooIndex", "/oak:index/", "/oak:index//fooIndex", "oak:index/fooIndex",
                "/:deleted/oak:index/barIndex", "/oak:index/:data")) {
            assertTrue(path, trail().getEntries(path).isEmpty());
        }
        assertEquals(1, trail().getEntries("/oak:index/fooIndex").size());
    }

    @Test
    public void entriesOfExistingNestedAndRemovedIndexes() {
        NodeBuilder foo = audit.child(INDEX_DEFINITIONS_NAME).child("fooIndex");
        entry(foo, "20261013T1405Z.10", "CHANGED");
        entry(foo, "20261013T1405Z.2", "CREATED");
        entry(audit.child("content").child(INDEX_DEFINITIONS_NAME).child("barIndex"), "20261013T1405Z.0", "CREATED");
        NodeBuilder removed = audit.child(DELETED_NODE_NAME).child(INDEX_DEFINITIONS_NAME).child("removedIndex");
        removed.setProperty(IndexDefinitionAudit.PROP_DELETED_AT, "2026-10-13T14:06:00.000Z", Type.DATE);
        entry(removed, "20261013T1406Z.0", "DELETED");

        Map<String, List<Entry>> byIndex = trail().getEntriesByIndex();

        assertEquals(List.of("/content/oak:index/barIndex", "/oak:index/fooIndex", "/oak:index/removedIndex"),
                List.copyOf(byIndex.keySet()));
        // oldest first, with the sequence number compared as a number
        assertEquals(List.of("CREATED", "CHANGED"), operations(byIndex.get("/oak:index/fooIndex")));
        assertEquals(List.of("DELETED"), operations(byIndex.get("/oak:index/removedIndex")));
        // newest first
        assertEquals(List.of("CHANGED", "CREATED"), operations(trail().getEntries("/oak:index/fooIndex")));
        assertEquals(List.of("DELETED"), operations(trail().getEntries("/oak:index/removedIndex")));
    }

    @Test
    public void entriesOfOneIndexDoNotReadOtherHistories() {
        entry(audit.child(INDEX_DEFINITIONS_NAME).child("fooIndex"), "20261013T1405Z.0", "CREATED");
        NodeBuilder definitions = audit.getChildNode(INDEX_DEFINITIONS_NAME);
        NodeBuilder bar = definitions.child("barIndex");
        entry(bar, "20261013T1405Z.0", "CREATED");
        // listing the entries of barIndex fails
        NodeState failing = Mockito.mock(NodeState.class, Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.doReturn(true).when(failing).exists();
        Mockito.doThrow(new IllegalStateException("test")).when(failing).getChildNodeEntries();
        NodeState root = Mockito.spy(this.root.getNodeState());
        NodeState oakIndex = Mockito.spy(root.getChildNode(INDEX_DEFINITIONS_NAME));
        NodeState auditNode = Mockito.spy(oakIndex.getChildNode(AUDIT_NODE_NAME));
        NodeState histories = Mockito.spy(auditNode.getChildNode(INDEX_DEFINITIONS_NAME));
        Mockito.doReturn(failing).when(histories).getChildNode("barIndex");
        // and listing all histories fails
        Mockito.doThrow(new IllegalStateException("test")).when(histories).getChildNodeEntries();
        Mockito.doThrow(new IllegalStateException("test")).when(auditNode).getChildNodeEntries();
        Mockito.doReturn(histories).when(auditNode).getChildNode(INDEX_DEFINITIONS_NAME);
        Mockito.doReturn(auditNode).when(oakIndex).getChildNode(AUDIT_NODE_NAME);
        Mockito.doReturn(oakIndex).when(root).getChildNode(INDEX_DEFINITIONS_NAME);

        assertEquals(List.of("CREATED"), operations(new IndexDefinitionAuditTrail(root).getEntries("/oak:index/fooIndex")));
    }

    @Test
    public void historyAndHistoryOfRemovedIndexWithTheSamePathAreMerged() {
        entry(audit.child(INDEX_DEFINITIONS_NAME).child("fooIndex"), "20261013T1500Z.0", "CREATED");
        NodeBuilder removed = audit.child(DELETED_NODE_NAME).child(INDEX_DEFINITIONS_NAME).child("fooIndex");
        entry(removed, "20261013T1405Z.0", "CREATED");
        entry(removed, "20261013T1406Z.0", "DELETED");

        assertEquals(List.of("CREATED", "DELETED", "CREATED"),
                operations(trail().getEntriesByIndex().get("/oak:index/fooIndex")));
        assertEquals(List.of("CREATED", "DELETED", "CREATED"), operations(trail().getEntries("/oak:index/fooIndex")));
    }

    @Test
    public void valuesOfAnyTypeAreReadAsStrings() {
        NodeBuilder foo = audit.child(INDEX_DEFINITIONS_NAME).child("fooIndex");
        foo.child("20261013T1405Z.0")
                .setProperty(PROP_OPERATION, "CHANGED")
                .setProperty(PROP_TIME, "2026-10-13T14:05:00.000Z", Type.DATE)
                .setProperty(PROP_USER, 42L)
                .setProperty(PROP_REASON, List.of("a", "b"), Type.STRINGS)
                .setProperty(PROP_THREAD, "main")
                .setProperty(PROP_DIFF, "+\"x\":1");
        // not read
        foo.child("20261013T1404Z.0").setProperty(PROP_OPERATION, "CREATED")
                .setProperty(PropertyStates.createProperty(PROP_DIFF, new ArrayBasedBlob(new byte[] {1}), Type.BINARY))
                .setProperty(unreadable(PROP_USER));
        // not entries: hidden nodes and names that are not entry names
        foo.child(":hidden").child("20261013T1405Z.1").setProperty(PROP_OPERATION, "X");
        foo.child("20261013T1405Z.99999999999999999999").setProperty(PROP_OPERATION, "X");

        List<Entry> entries = trail().getEntries("/oak:index/fooIndex");

        assertEquals(List.of(
                new Entry("/oak:index/fooIndex", "20261013T1405Z.0", "CHANGED", "2026-10-13T14:05:00.000Z",
                        "42", "", "main", "+\"x\":1"),
                new Entry("/oak:index/fooIndex", "20261013T1404Z.0", "CREATED", "", "", "", "", "")), entries);
        // an entry without values
        foo.child("20261013T1406Z.0");
        assertEquals(new Entry("/oak:index/fooIndex", "20261013T1406Z.0", "", "", "", "", "", ""),
                trail().getEntries("/oak:index/fooIndex").get(0));
    }

    /**
     * A property whose value can not be read (for example from a broken store).
     */
    private static PropertyState unreadable(String name) {
        PropertyState property = Mockito.mock(PropertyState.class);
        Mockito.when(property.getName()).thenReturn(name);
        Mockito.when(property.getType()).thenReturn((Type) Type.STRING);
        Mockito.when(property.getValue(Type.STRING)).thenThrow(new IllegalStateException("test"));
        return property;
    }

    private IndexDefinitionAuditTrail trail() {
        return new IndexDefinitionAuditTrail(root.getNodeState());
    }

    private static void entry(NodeBuilder history, String name, String operation) {
        history.child(name).setProperty(PROP_OPERATION, operation);
    }

    private static List<String> operations(List<Entry> entries) {
        return entries.stream().map(Entry::operation).collect(Collectors.toList());
    }
}
