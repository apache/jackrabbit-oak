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
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ORIGINAL_TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.AUDIT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DELETED_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DISABLED_BY;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DISABLED_REASON;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.ENTRY_ORDER;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_DIFF;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_OPERATION;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_REASON;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_THREAD;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_TIME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.PROP_USER;

import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.function.Consumer;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.RecursiveDelete;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.ResetCommitAttributeHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Disables, enables and deletes index definitions, recording the changes with
 * {@link IndexDefinitionAudit}, and reads the audit trail.
 * <p>
 * Changes of index definitions are committed with the index update of the repository
 * (so that for example the node type index is updated, and synchronous indexes are
 * reindexed in the same commit), which also records them. The index data of a deleted
 * index is removed in batches before, without the index update (hidden nodes are not
 * indexed).
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12458">OAK-12458</a>
 */
public class IndexManagerMBeanImpl extends AnnotatedStandardMBean implements IndexManagerMBean {

    private static final Logger LOG = LoggerFactory.getLogger(IndexManagerMBeanImpl.class);

    /**
     * Commit origin ({@link CommitInfo#getSessionId()}) of the changes done here.
     */
    static final String ORIGIN = "IndexManager";

    static final String UNKNOWN_CALLER = "unknown";

    /*
     * Properties set when an index is disabled, as also set by the index management tools.
     */
    static final String ORIGINAL_TYPE = "originalType";
    static final String DISABLED_AT = "disabledAt";

    /**
     * For removing index data (hidden nodes, which are not indexed).
     */
    private static final CommitHook INDEX_DATA_HOOK = new CompositeHook(
            new ConflictHook(new AnnotatingConflictHandler()), new EditorHook(new ConflictValidatorProvider()));

    /*
     * JMX clients show the items of a row sorted by name, so the names are numbered to
     * keep them in this order.
     */
    private static final String[] INDEX_ITEMS = {
            "1. index", "2. lastOperation", "3. lastTime", "4. entries", "5. lastUser"};

    private static final OpenType<?>[] INDEX_TYPES = {
            SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.INTEGER, SimpleType.STRING};

    private static final String[] ENTRY_ITEMS = {
            "1. operation", "2. time", "3. user", "4. reason", "5. thread", "6. diff"};

    private static final OpenType<?>[] ENTRY_TYPES = {
            SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
            SimpleType.STRING};

    private final NodeStore store;

    private final IndexUpdateProvider indexUpdate;

    private final CommitHook indexUpdateHook;

    /**
     * @param store        the node store
     * @param indexEditors the index editors of the repository
     * @param feature      the feature toggle that enables the index definition audit; changes
     *                     are only possible if the audit is enabled (also with the system
     *                     property {@link IndexDefinitionAudit#ENABLED_PROPERTY})
     */
    public IndexManagerMBeanImpl(NodeStore store, IndexEditorProvider indexEditors, @Nullable Feature feature) {
        super(IndexManagerMBean.class);
        this.store = store;
        this.indexUpdate = new IndexUpdateProvider(indexEditors);
        indexUpdate.setIndexDefinitionAuditFeature(feature);
        this.indexUpdateHook = new CompositeHook(
                ResetCommitAttributeHook.INSTANCE,
                new EditorHook(indexUpdate),
                new ConflictHook(new AnnotatingConflictHandler()),
                new EditorHook(new ConflictValidatorProvider()));
    }

    @Override
    public TabularData getIndexes() throws OpenDataException {
        CompositeType rowType = new CompositeType("IndexDefinitionAuditIndex", "Audited index",
                INDEX_ITEMS, INDEX_ITEMS, INDEX_TYPES);
        TabularDataSupport result = new TabularDataSupport(
                new TabularType("IndexDefinitionAuditIndexes", "Audited indexes", rowType, new String[] {INDEX_ITEMS[0]}));
        // an index can have both a history and a history of a removed index with the same path,
        // if it was created again while the audit was disabled
        Map<String, List<Entry>> entriesByPath = new TreeMap<>();
        for (History history : getHistories()) {
            history.addEntries(entriesByPath.computeIfAbsent(history.path, p -> new ArrayList<>()));
        }
        for (Map.Entry<String, List<Entry>> e : entriesByPath.entrySet()) {
            NodeState last = Collections.max(e.getValue(), Comparator.comparing((Entry entry) -> entry.name, ENTRY_ORDER)).state;
            result.put(new CompositeDataSupport(rowType, INDEX_ITEMS, new Object[] {
                    e.getKey(), getString(last, PROP_OPERATION), getDate(last, PROP_TIME),
                    e.getValue().size(), getString(last, PROP_USER)}));
        }
        return result;
    }

    @Override
    public TabularData getAuditTrail(String indexPath) throws OpenDataException {
        String path = toIndexPath(indexPath);
        List<Entry> entries = new ArrayList<>();
        for (History history : getHistories()) {
            if (history.path.equals(path)) {
                history.addEntries(entries);
            }
        }
        return toTable(entries);
    }

    @Override
    public String disableIndex(String indexPath, String by, String reason, boolean force) {
        checkEnabled();
        String path = toIndexPath(indexPath);
        NodeState definition = getIndexDefinition(path);
        String type = definition.getString(TYPE_PROPERTY_NAME);
        if (TYPE_DISABLED.equals(type)) {
            return path + " is already disabled";
        }
        checkNotSynchronous(path, definition, force);
        String caller = toCaller(by);
        String why = toReason(reason);
        commit("disableIndex", path, caller, why, null, builder -> {
            builder.setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED);
            builder.setProperty(ORIGINAL_TYPE, type);
            builder.setProperty(DISABLED_AT, Instant.now().toString());
            builder.setProperty(DISABLED_BY, caller);
            if (why == null) {
                builder.removeProperty(DISABLED_REASON);
            } else {
                builder.setProperty(DISABLED_REASON, why);
            }
        });
        LOG.info("Disabled index {} (by {}, reason: {})", path, caller, why);
        return "Disabled " + path;
    }

    @Override
    public String enableIndex(String indexPath, String by, boolean force) {
        checkEnabled();
        String path = toIndexPath(indexPath);
        NodeState definition = getIndexDefinition(path);
        if (!TYPE_DISABLED.equals(definition.getString(TYPE_PROPERTY_NAME))) {
            return path + " is not disabled";
        }
        // a synchronous index is reindexed in this commit
        checkNotSynchronous(path, definition, force);
        String type = definition.getString(ORIGINAL_TYPE);
        if (type == null || type.isBlank()) {
            // set by the purge of old index versions (oak-run)
            type = definition.getString(ORIGINAL_TYPE_PROPERTY_NAME);
        }
        if (type == null || type.isBlank()) {
            throw new IllegalStateException("The type of " + path + " before it was disabled is unknown");
        }
        String originalType = type;
        String caller = toCaller(by);
        commit("enableIndex", path, caller, null, null, builder -> {
            builder.setProperty(TYPE_PROPERTY_NAME, originalType);
            for (String name : List.of(ORIGINAL_TYPE, ORIGINAL_TYPE_PROPERTY_NAME, DISABLED_AT, DISABLED_BY,
                    DISABLED_REASON)) {
                builder.removeProperty(name);
            }
            // a disabled index is not updated, and its data may have been removed (purge)
            builder.setProperty(REINDEX_PROPERTY_NAME, true);
        });
        LOG.info("Enabled index {} with type {} (by {}), reindexing", path, originalType, caller);
        return "Enabled " + path + " (type " + originalType + "), reindexing";
    }

    @Override
    public String deleteIndex(String indexPath, String by, String reason, boolean force) {
        checkEnabled();
        String path = toIndexPath(indexPath);
        NodeState definition = getIndexDefinition(path);
        if (!TYPE_DISABLED.equals(definition.getString(TYPE_PROPERTY_NAME))) {
            throw new IllegalStateException(path + " must be disabled before it is deleted");
        }
        checkNotSynchronous(path, definition, force);
        String caller = toCaller(by);
        // the index data can be large: delete it in batches first, then the definition in one commit
        List<String> indexData = new ArrayList<>();
        for (String name : definition.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(name)) {
                indexData.add(concat(path, name));
            }
        }
        // the index must stay disabled while it is deleted (it could be enabled concurrently)
        CommitHook stillDisabled = (before, after, info) -> {
            if (!TYPE_DISABLED.equals(NodeStateUtils.getNode(before, path).getString(TYPE_PROPERTY_NAME))) {
                throw new CommitFailedException(CommitFailedException.STATE, 1,
                        path + " is no longer disabled, it is not deleted");
            }
            return after;
        };
        RecursiveDelete delete = new RecursiveDelete(store, new CompositeHook(stillDisabled, INDEX_DATA_HOOK),
                () -> new CommitInfo(ORIGIN, caller));
        if (!indexData.isEmpty()) {
            try {
                delete.run(indexData);
            } catch (CommitFailedException e) {
                throw new IllegalStateException("Failed to delete the index data of " + path + ": " + e.getMessage(), e);
            }
        }
        String why = toReason(reason);
        commit("deleteIndex", path, caller, why, stillDisabled, NodeBuilder::remove);
        LOG.info("Deleted index {} (by {}, reason: {}): removed {} index data nodes in {} commits", path, caller,
                why, delete.getNumRemoved(), delete.getMergeCount());
        return "Deleted " + path + " (" + delete.getNumRemoved() + " index data nodes removed in "
                + delete.getMergeCount() + " commits)";
    }

    private void checkEnabled() {
        // changes are only allowed if this MBean's own commits are recorded
        if (!indexUpdate.isIndexDefinitionAuditEnabled()) {
            throw new IllegalStateException("Changing index definitions requires the index definition audit "
                    + "(feature toggle " + IndexUpdateProvider.FT_OAK_12458 + ")");
        }
    }

    private static String toIndexPath(String indexPath) {
        String path = indexPath == null ? "" : indexPath.trim();
        return path.startsWith("/") ? path : concat("/" + INDEX_DEFINITIONS_NAME, path);
    }

    /**
     * Synchronous indexes (without async lane, for example the uuid, node type and
     * reference index) are used by every commit; they are only changed if forced.
     */
    private static void checkNotSynchronous(String path, NodeState definition, boolean force) {
        if (!force && !definition.hasProperty(ASYNC_PROPERTY_NAME)) {
            throw new IllegalStateException(path + " is a synchronous index: set force to change it");
        }
    }

    /**
     * Returns who changes the index definition: the given value, else the principals of the
     * JMX caller (if known), else {@link #UNKNOWN_CALLER}.
     */
    private static String toCaller(String by) {
        if (by != null && !by.isBlank()) {
            return by.trim();
        }
        Subject subject = Java23Subject.getSubject();
        if (subject != null && !subject.getPrincipals().isEmpty()) {
            return subject.getPrincipals().stream().map(Principal::getName).sorted()
                    .collect(Collectors.joining(","));
        }
        return UNKNOWN_CALLER;
    }

    private static String toReason(String reason) {
        return reason == null || reason.isBlank() ? null : reason.trim();
    }

    private NodeState getIndexDefinition(String path) {
        NodeState definition = NodeStateUtils.getNode(store.getRoot(), path);
        // only index definitions directly below /oak:index
        if (!definition.exists() || PathUtils.denotesRoot(path)
                || !("/" + INDEX_DEFINITIONS_NAME).equals(PathUtils.getParentPath(path))
                || !definition.hasProperty(TYPE_PROPERTY_NAME)) {
            throw new IllegalArgumentException("Not an index definition below /" + INDEX_DEFINITIONS_NAME + ": " + path);
        }
        return definition;
    }

    /**
     * Changes an index definition in one commit with the index update, which records the change.
     */
    private void commit(String operation, String path, String caller, String reason, @Nullable CommitHook check,
            Consumer<NodeBuilder> change) {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder definition = builder;
        for (String name : PathUtils.elements(path)) {
            definition = definition.getChildNode(name);
        }
        change.accept(definition);
        Map<String, Object> info = new HashMap<>();
        info.put(CommitContext.NAME, new SimpleCommitContext());
        info.put(IndexDefinitionAudit.COMMIT_INFO_THREAD, ORIGIN + "." + operation);
        if (reason != null) {
            info.put(IndexDefinitionAudit.COMMIT_INFO_REASON, reason);
        }
        try {
            CommitHook hook = check == null ? indexUpdateHook : new CompositeHook(check, indexUpdateHook);
            store.merge(builder, hook, new CommitInfo(ORIGIN, caller, info));
        } catch (CommitFailedException e) {
            throw new IllegalStateException("Failed to change " + path + ": " + e.getMessage(), e);
        }
    }

    private TabularData toTable(List<Entry> entries) throws OpenDataException {
        CompositeType rowType = new CompositeType("IndexDefinitionAuditEntry", "Audit entry",
                ENTRY_ITEMS, ENTRY_ITEMS, ENTRY_TYPES);
        TabularDataSupport result = new TabularDataSupport(
                new TabularType("IndexDefinitionAuditEntries", "Audit entries", rowType,
                        ENTRY_ITEMS));
        // newest first
        entries.sort(Comparator.comparing((Entry e) -> e.name, ENTRY_ORDER).reversed()
                .thenComparing(e -> e.history.path));
        for (Entry entry : entries) {
            CompositeDataSupport row = new CompositeDataSupport(rowType, ENTRY_ITEMS, entry.values());
            // the rows have no key column: identical rows (same change at the same time) are shown once
            if (!result.containsKey(result.calculateIndex(row))) {
                result.put(row);
            }
        }
        return result;
    }

    private List<History> getHistories() {
        NodeState audit = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(AUDIT_NODE_NAME);
        List<History> histories = new ArrayList<>();
        collect(audit, "/", histories);
        collect(audit.getChildNode(DELETED_NODE_NAME), "/", histories);
        return histories;
    }

    private static void collect(NodeState state, String path, List<History> histories) {
        boolean hasEntries = false;
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            String name = child.getName();
            if (IndexDefinitionAudit.isEntryName(name)) {
                hasEntries = true;
            } else if (!NodeStateUtils.isHidden(name)) {
                collect(child.getNodeState(), concat(path, name), histories);
            }
        }
        if (hasEntries) {
            histories.add(new History(path, state));
        }
    }

    private static String getString(NodeState state, String name) {
        String value = state.getString(name);
        return value == null ? "" : value;
    }

    private static String getDate(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        return property == null ? "" : property.getValue(Type.DATE);
    }

    private static final class History {
        final String path;
        final NodeState state;

        History(String path, NodeState state) {
            this.path = path;
            this.state = state;
        }

        List<String> getEntryNames() {
            List<String> names = new ArrayList<>();
            for (String name : state.getChildNodeNames()) {
                if (IndexDefinitionAudit.isEntryName(name)) {
                    names.add(name);
                }
            }
            names.sort(ENTRY_ORDER);
            return names;
        }

        void addEntries(List<Entry> entries) {
            for (String name : getEntryNames()) {
                entries.add(new Entry(this, name, state.getChildNode(name)));
            }
        }
    }

    private static final class Entry {
        final History history;
        final String name;
        final NodeState state;

        Entry(History history, String name, NodeState state) {
            this.history = history;
            this.name = name;
            this.state = state;
        }

        Object[] values() {
            return new Object[] {
                    getString(state, PROP_OPERATION), getDate(state, PROP_TIME), getString(state, PROP_USER),
                    getString(state, PROP_REASON), getString(state, PROP_THREAD), getString(state, PROP_DIFF)};
        }
    }
}
