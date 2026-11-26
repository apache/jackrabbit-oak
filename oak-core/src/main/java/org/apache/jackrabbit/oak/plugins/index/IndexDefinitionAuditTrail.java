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

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.AUDIT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DELETED_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.ENTRY_ORDER;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_DIFF;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_OPERATION;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_REASON;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_THREAD;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_TIME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_USER;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

/**
 * Reads the audit trail written by {@link IndexDefinitionAudit}: the histories of the
 * existing and of removed indexes.
 */
final class IndexDefinitionAuditTrail {

    /**
     * An entry of the audit trail. Missing values are empty strings.
     */
    record Entry(String indexPath, String name, String operation, String time, String user, String reason,
            String thread, String diff) {
    }

    private final NodeState root;

    /**
     * @param root the root node state to read the audit trail from
     */
    IndexDefinitionAuditTrail(NodeState root) {
        this.root = root;
    }

    /**
     * Returns the entries of all indexes with an audit trail, by index path, oldest entry first.
     * An index can have both a history and a history of a removed index with the same path
     * (if it was created again while the audit was disabled): their entries are merged.
     */
    SortedMap<String, List<Entry>> getEntriesByIndex() {
        SortedMap<String, List<Entry>> result = new TreeMap<>();
        for (Entry entry : getEntries()) {
            result.computeIfAbsent(entry.indexPath(), p -> new ArrayList<>()).add(entry);
        }
        result.values().forEach(entries -> entries.sort(Comparator.comparing(Entry::name, ENTRY_ORDER)));
        return result;
    }

    /**
     * Returns the entries of an index (also of a removed index), newest entry first.
     */
    List<Entry> getEntries(String indexPath) {
        List<Entry> result = new ArrayList<>();
        if (isIndexPath(indexPath)) {
            NodeState audit = root.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(AUDIT_NODE_NAME);
            addEntries(NodeStateUtils.getNode(audit, indexPath), indexPath, result);
            addEntries(NodeStateUtils.getNode(audit.getChildNode(DELETED_NODE_NAME), indexPath), indexPath, result);
        }
        result.sort(Comparator.comparing(Entry::name, ENTRY_ORDER).reversed());
        return result;
    }

    /**
     * Whether the path can be the path of an index: absolute, and without hidden names (such
     * as the removed histories in the audit trail itself).
     */
    private static boolean isIndexPath(String path) {
        return PathUtils.isValid(path) && PathUtils.isAbsolute(path) && !PathUtils.denotesRoot(path)
                && !NodeStateUtils.isHiddenPath(path);
    }

    private static void addEntries(NodeState history, String indexPath, List<Entry> entries) {
        for (ChildNodeEntry child : history.getChildNodeEntries()) {
            if (IndexDefinitionAudit.isEntryName(child.getName())) {
                entries.add(toEntry(indexPath, child.getName(), child.getNodeState()));
            }
        }
    }

    private List<Entry> getEntries() {
        NodeState audit = root.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(AUDIT_NODE_NAME);
        List<Entry> entries = new ArrayList<>();
        collect(audit, "/", entries);
        collect(audit.getChildNode(DELETED_NODE_NAME), "/", entries);
        return entries;
    }

    private static void collect(NodeState state, String path, List<Entry> entries) {
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            String name = child.getName();
            if (IndexDefinitionAudit.isEntryName(name)) {
                entries.add(toEntry(path, name, child.getNodeState()));
            } else if (!NodeStateUtils.isHidden(name)) {
                collect(child.getNodeState(), concat(path, name), entries);
            }
        }
    }

    private static Entry toEntry(String indexPath, String name, NodeState state) {
        return new Entry(indexPath, name, getString(state, PROP_OPERATION), getString(state, PROP_TIME),
                getString(state, PROP_USER), getString(state, PROP_REASON), getString(state, PROP_THREAD),
                getString(state, PROP_DIFF));
    }

    /**
     * Returns the value as a string, also of an entry edited by hand: empty if missing, or
     * if it is not a single value that can be read as a string (binaries are not read).
     */
    private static String getString(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        if (property == null || property.isArray() || property.getType() == Type.BINARY) {
            return "";
        }
        try {
            return property.getValue(Type.STRING);
        } catch (RuntimeException e) {
            return "";
        }
    }
}
