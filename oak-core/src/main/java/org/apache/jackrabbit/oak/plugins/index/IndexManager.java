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
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DISABLED_BY;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.DISABLED_REASON;
import static org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit.MAX_VALUE_LENGTH;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getBuilder;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Disables, enables and deletes index definitions below {@code /oak:index}.
 * <p>
 * Changes of index definitions are committed with the index update of the repository
 * (so that for example the node type index is updated, and synchronous indexes are
 * reindexed in the same commit), which also records them with {@link IndexDefinitionAudit}.
 * The index data of a deleted index is removed in batches before, without the index update
 * (hidden nodes are not indexed).
 */
final class IndexManager {

    private static final Logger LOG = LoggerFactory.getLogger(IndexManager.class);

    /**
     * Commit origin ({@link CommitInfo#getSessionId()}) of the changes done here.
     */
    static final String ORIGIN = "IndexManager";

    /*
     * Properties set when an index is disabled, as also set by the index management tools.
     */
    static final String ORIGINAL_TYPE = "originalType";
    static final String DISABLED_AT = "disabledAt";

    /**
     * For changes without the index update: removing index data (hidden nodes, which are not
     * indexed), and the reindex flag of a disabled index.
     */
    private static final CommitHook INDEX_DATA_HOOK = new CompositeHook(
            new ConflictHook(new AnnotatingConflictHandler()), new EditorHook(new ConflictValidatorProvider()));

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
    IndexManager(NodeStore store, IndexEditorProvider indexEditors, @Nullable Feature feature) {
        this.store = store;
        this.indexUpdate = new IndexUpdateProvider(indexEditors);
        indexUpdate.setIndexDefinitionAuditFeature(feature);
        this.indexUpdateHook = new CompositeHook(
                ResetCommitAttributeHook.INSTANCE,
                new EditorHook(indexUpdate),
                new ConflictHook(new AnnotatingConflictHandler()),
                new EditorHook(new ConflictValidatorProvider()));
    }

    /**
     * @param path   path of the index definition, directly below {@code /oak:index}
     * @param caller who disables it
     * @param reason why, or null
     * @param force  also disable a synchronous index
     */
    String disable(String path, @NotNull String caller, @Nullable String reason, boolean force) {
        checkEnabled();
        checkLength("by", caller);
        String why = toReason(reason);
        NodeState definition = getIndexDefinition(path);
        String type = definition.getString(TYPE_PROPERTY_NAME);
        if (TYPE_DISABLED.equals(type)) {
            return path + " is already disabled";
        }
        checkNotSynchronous(path, definition, force);
        commit("disableIndex", path, caller, why, unchanged(path, definition, "disabled"), builder -> {
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

    /**
     * @param path   path of the index definition, directly below {@code /oak:index}
     * @param caller who enables it
     * @param force  also enable (and so reindex in this call) a synchronous index
     */
    String enable(String path, @NotNull String caller, boolean force) {
        checkEnabled();
        checkLength("by", caller);
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
        commit("enableIndex", path, caller, null, unchanged(path, definition, "enabled"), builder -> {
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

    /**
     * @param path   path of the disabled index definition, directly below {@code /oak:index}
     * @param caller who deletes it
     * @param reason why, or null
     * @param force  also delete a synchronous index
     */
    String delete(String path, @NotNull String caller, @Nullable String reason, boolean force) {
        checkEnabled();
        checkLength("by", caller);
        String why = toReason(reason);
        NodeState definition = getIndexDefinition(path);
        if (!TYPE_DISABLED.equals(definition.getString(TYPE_PROPERTY_NAME))) {
            throw new IllegalStateException(path + " must be disabled before it is deleted");
        }
        checkNotSynchronous(path, definition, force);
        // the index must stay disabled and unchanged while it is deleted (it could be enabled
        // or replaced concurrently)
        CommitHook unchanged = unchanged(path, definition, "deleted");
        List<String> indexData = getIndexData(path);
        // if the index is enabled in another way while its data is deleted, it is reindexed:
        // the reindex flag must stay set
        CommitHook reindexing = (before, after, info) -> {
            if (!NodeStateUtils.getNode(before, path).getBoolean(REINDEX_PROPERTY_NAME)) {
                throw new CommitFailedException(CommitFailedException.STATE, 3,
                        path + " is no longer marked for reindexing, it is not deleted");
            }
            return after;
        };
        CommitHook check = indexData.isEmpty() ? unchanged : new CompositeHook(unchanged, reindexing);
        RecursiveDelete delete = new RecursiveDelete(store, new CompositeHook(check, INDEX_DATA_HOOK),
                () -> new CommitInfo(ORIGIN, caller));
        if (!indexData.isEmpty()) {
            try {
                if (!definition.getBoolean(REINDEX_PROPERTY_NAME)) {
                    NodeBuilder builder = store.getRoot().builder();
                    getBuilder(builder, path).setProperty(REINDEX_PROPERTY_NAME, true);
                    store.merge(builder, new CompositeHook(unchanged, INDEX_DATA_HOOK), new CommitInfo(ORIGIN, caller));
                }
                // the index data can be large: delete it in batches first, then the definition in one commit
                delete.run(indexData);
            } catch (CommitFailedException e) {
                throw new IllegalStateException("Failed to delete the index data of " + path + ": " + e.getMessage()
                        + partlyRemoved(delete), e);
            }
        }
        try {
            commit("deleteIndex", path, caller, why, check, NodeBuilder::remove);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(e.getMessage() + partlyRemoved(delete), e);
        }
        LOG.info("Deleted index {} (by {}, reason: {}): removed {} index data nodes in {} commits", path, caller,
                why, delete.getNumRemoved(), delete.getMergeCount());
        return "Deleted " + path + " (" + delete.getNumRemoved() + " index data nodes removed in "
                + delete.getMergeCount() + " commits)";
    }

    /**
     * Returns a check that the index definition is not changed between reading it and the
     * commit (a concurrent change would otherwise be overwritten, or a stale state recorded).
     * Its index data and the reindex flag may change.
     *
     * @param operation the operation, for the error message
     */
    private static CommitHook unchanged(String path, NodeState definition, String operation) {
        NodeState expected = withoutIndexData(definition);
        boolean disabled = TYPE_DISABLED.equals(definition.getString(TYPE_PROPERTY_NAME));
        return (before, after, info) -> {
            NodeState current = NodeStateUtils.getNode(before, path);
            if (disabled && !TYPE_DISABLED.equals(current.getString(TYPE_PROPERTY_NAME))) {
                throw new CommitFailedException(CommitFailedException.STATE, 1,
                        path + " is no longer disabled, it is not " + operation);
            }
            if (!withoutIndexData(current).equals(expected)) {
                throw new CommitFailedException(CommitFailedException.STATE, 2,
                        path + " was changed meanwhile, it is not " + operation);
            }
            return after;
        };
    }

    /**
     * Tells if index data was removed before a delete failed (only committed batches count).
     */
    private static String partlyRemoved(RecursiveDelete delete) {
        return delete.getMergeCount() == 0 ? ""
                : " (index data was partly removed in " + delete.getMergeCount()
                        + " commits: if the index is used, it must be reindexed)";
    }

    /**
     * Returns the index definition as read by the operations: without its index data (hidden
     * nodes), the reindex flag (set by enable and delete) and the reindex count.
     */
    private static NodeState withoutIndexData(NodeState definition) {
        NodeBuilder copy = EMPTY_NODE.builder();
        IndexDefinitionAudit.copyVisible(definition, copy);
        // changed by the index update
        copy.removeProperty(REINDEX_PROPERTY_NAME);
        copy.removeProperty(REINDEX_COUNT);
        // hidden, but read by enable (set by the purge of old index versions)
        PropertyState originalType = definition.getProperty(ORIGINAL_TYPE_PROPERTY_NAME);
        if (originalType != null) {
            copy.setProperty(originalType);
        }
        return copy.getNodeState();
    }

    /**
     * Returns the paths of the index data (hidden child nodes) that can be removed: not the
     * data in read-only mounts (composite node store), as also kept on reindex.
     */
    private List<String> getIndexData(String path) {
        NodeBuilder definition = getBuilder(store.getRoot().builder(), path);
        List<String> indexData = new ArrayList<>();
        for (String name : definition.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(name) && !(definition.getChildNode(name) instanceof ReadOnlyBuilder)) {
                indexData.add(concat(path, name));
            }
        }
        return indexData;
    }

    private void checkEnabled() {
        // changes are only allowed if the changes done here are recorded
        if (!indexUpdate.isIndexDefinitionAuditEnabled()) {
            throw new IllegalStateException("Changing index definitions requires the index definition audit "
                    + "(feature toggle " + IndexUpdateProvider.FT_OAK_12458 + ")");
        }
    }

    private static void checkLength(String name, String value) {
        if (value != null && value.length() > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException(name + " is longer than " + MAX_VALUE_LENGTH + " characters");
        }
    }

    private static String toReason(String reason) {
        String why = reason == null || reason.isBlank() ? null : reason.trim();
        checkLength("reason", why);
        return why;
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

    private NodeState getIndexDefinition(String path) {
        if (!PathUtils.isValid(path) || !PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("Not a valid path: " + path);
        }
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
    private void commit(String operation, String path, String caller, String reason, CommitHook check,
            Consumer<NodeBuilder> change) {
        NodeBuilder builder = store.getRoot().builder();
        change.accept(getBuilder(builder, path));
        Map<String, Object> info = IndexDefinitionAudit.commitInfo(ORIGIN + "." + operation, reason);
        info.put(CommitContext.NAME, new SimpleCommitContext());
        try {
            store.merge(builder, new CompositeHook(check, indexUpdateHook), new CommitInfo(ORIGIN, caller, info));
        } catch (CommitFailedException e) {
            throw new IllegalStateException("Failed to change " + path + ": " + e.getMessage(), e);
        }
    }
}
