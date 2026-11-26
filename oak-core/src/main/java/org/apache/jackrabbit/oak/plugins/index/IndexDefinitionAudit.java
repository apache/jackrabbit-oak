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
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REFRESH_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.childBuilder;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getBuilder;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndexMerger;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncLaneSwitcher;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records changes of index definitions in the repository (in the same commit), so
 * that it is possible to find out when, by whom and how an index definition was
 * changed. The entries can be read with {@link IndexManagerMBean}.
 * <p>
 * The history of an index is stored in the hidden node {@code /oak:index/:audit},
 * which mirrors the index path, for example
 * {@code /oak:index/:audit/oak:index/damAssetLucene-12/20261013T1405Z.0}. Entries
 * are named after the minute (UTC) of the change and a sequence number within
 * that minute. When an index is removed, its history is moved to
 * {@code /oak:index/:audit/:deleted/<index path>}, and moved back if an index with
 * the same path is created again.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12458">OAK-12458</a>
 */
public final class IndexDefinitionAudit {

    private static final Logger LOG = LoggerFactory.getLogger(IndexDefinitionAudit.class);

    /**
     * System property to enable the audit for all index updates in this JVM, including
     * tools (e.g. oak-run) where the feature toggle is not available.
     */
    public static final String ENABLED_PROPERTY = "oak.indexDefinitionAudit";

    /**
     * Commit origin ({@link CommitInfo#getSessionId()}) of the index importer. An import of
     * an existing index rebuilds it, so it is recorded as {@link Operation#REINDEX}.
     */
    public static final String ORIGIN_INDEX_IMPORTER = "IndexImporter";

    /**
     * Key in the {@link CommitInfo#getInfo() commit info} for why the index definitions are
     * changed (for example by {@link IndexManagerMBean}).
     */
    static final String COMMIT_INFO_REASON = "oak.indexDefinitionAudit.reason";

    /**
     * Key in the {@link CommitInfo#getInfo() commit info} for what is recorded as the thread,
     * for changes where the name of the current thread tells nothing (for example JMX calls).
     */
    static final String COMMIT_INFO_THREAD = "oak.indexDefinitionAudit.thread";

    static final String AUDIT_NODE_NAME = ":audit";

    static final String DELETED_NODE_NAME = ":deleted";

    static final int MAX_ENTRIES = Integer.getInteger("oak.indexDefinitionAudit.maxEntries", 1000);

    static final int MAX_DELETED = Integer.getInteger("oak.indexDefinitionAudit.maxDeleted", 1000);

    static final int MAX_DIFF_LENGTH = 2048;

    /**
     * Maximum length of the recorded user, reason and thread.
     */
    static final int MAX_VALUE_LENGTH = 1024;

    static final String PROP_PATH = "path";
    static final String PROP_OPERATION = "operation";
    static final String PROP_USER = "user";
    static final String PROP_ORIGIN = "origin";
    static final String PROP_TIME = "time";
    static final String PROP_THREAD = "thread";
    static final String PROP_COMMIT_PATH = "commitPath";
    static final String PROP_DIFF = "diff";
    static final String PROP_REINDEX_REQUESTED = "reindexRequested";
    static final String PROP_DELETED_AT = "deletedAt";
    static final String PROP_DIFF_BASE = "diffBase";
    static final String PROP_REASON = "reason";

    private static final DateTimeFormatter MINUTE_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmm'Z'").withZone(ZoneOffset.UTC);

    // the sequence number fits into a long
    private static final Pattern ENTRY_NAME = Pattern.compile("(\\d{8}T\\d{4}Z)\\.(\\d{1,18})");

    static final Comparator<String> ENTRY_ORDER = Comparator
            .comparing((String name) -> entryNameMatcher(name).group(1))
            .thenComparingLong(name -> Long.parseLong(entryNameMatcher(name).group(2)));

    enum Operation { CREATED, MERGED, CHANGED, DELETED, REINDEX, DISABLED, ENABLED }

    /**
     * Property set on index definitions that are the result of merging a product index
     * with a customization (see {@code DiffIndexMerger} and {@code IndexDefMergerUtils}).
     */
    static final String MERGES_PROPERTY = "merges";

    /**
     * Set by tools before they delete an index definition, to tell who deleted it.
     */
    static final String DELETED_BY = "deletedBy";

    /**
     * Set by tools when they disable an index definition, to tell who disabled it.
     */
    static final String DISABLED_BY = "disabledBy";

    /**
     * Set by the diff index merger on merged index definitions (a fixed text).
     */
    static final String MERGE_INFO_PROPERTY = "mergeInfo";

    /**
     * Set by tools when they disable an index definition, to tell why it was disabled.
     */
    static final String DISABLED_REASON = "disabledReason";

    /**
     * Properties that are internal bookkeeping of the index implementation (or set by
     * tools to tell who changed the index) and not part of the definition as configured
     * by the user.
     */
    private static final Set<String> IGNORED_PROPERTIES = Set.of(REINDEX_COUNT, "seed", REFRESH_PROPERTY_NAME,
            DELETED_BY, DiffIndexMerger.MERGE_CHECKSUM, MERGE_INFO_PROPERTY);

    /**
     * Nodes below {@code /oak:index} that are not index definitions themselves, but used
     * to generate them. The generated index definitions are recorded instead.
     */
    private static final Set<String> IGNORED_NODES = Set.of(
            DiffIndexMerger.DIFF_INDEX, DiffIndexMerger.DIFF_INDEX_OPTIMIZER);

    private final CommitInfo commitInfo;

    private final NodeBuilder rootBuilder;

    private final int maxEntries;

    private final int maxDeleted;

    private final Clock clock;

    private final String reason;

    private final String thread;

    private boolean deletedHistoryAdded;

    IndexDefinitionAudit(CommitInfo commitInfo, NodeBuilder rootBuilder, int maxEntries, int maxDeleted) {
        this(commitInfo, rootBuilder, maxEntries, maxDeleted, Clock.systemUTC());
    }

    IndexDefinitionAudit(CommitInfo commitInfo, NodeBuilder rootBuilder, int maxEntries, int maxDeleted, Clock clock) {
        this.commitInfo = commitInfo == null ? CommitInfo.EMPTY : commitInfo;
        this.rootBuilder = rootBuilder;
        this.maxEntries = Math.max(1, maxEntries);
        this.maxDeleted = Math.max(1, maxDeleted);
        this.clock = clock;
        this.reason = getInfo(this.commitInfo, COMMIT_INFO_REASON);
        this.thread = getInfo(this.commitInfo, COMMIT_INFO_THREAD);
    }

    /**
     * Returns the {@link CommitInfo#getInfo() commit info} entries that tell the audit
     * what to record as the thread and why the index definitions are changed.
     *
     * @param thread what to record as the thread
     * @param reason why the index definitions are changed, or null
     * @return a new, modifiable map
     */
    static Map<String, Object> commitInfo(String thread, @Nullable String reason) {
        Map<String, Object> info = new HashMap<>();
        info.put(COMMIT_INFO_THREAD, thread);
        if (reason != null) {
            info.put(COMMIT_INFO_REASON, reason);
        }
        return info;
    }

    private static String getInfo(CommitInfo commitInfo, String key) {
        Object value = commitInfo.getInfo().get(key);
        return value instanceof String ? (String) value : null;
    }

    /**
     * Records all visible changes of the index definitions below the given
     * {@code oak:index} node.
     *
     * @param definitionsPath path of the {@code oak:index} node
     * @param before          state of the {@code oak:index} node before the commit
     * @param definitions     builder of the {@code oak:index} node, including the changes
     *                        done by the commit
     */
    void audit(String definitionsPath, NodeState before, NodeBuilder definitions) {
        if (!before.exists() && !definitions.exists()) {
            return;
        }
        NodeState after = definitions.getNodeState();
        Set<String> changed = new TreeSet<>();
        after.compareAgainstBaseState(before, new DefaultNodeStateDiff() {
            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                return collect(name);
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                return collect(name);
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                return collect(name);
            }

            private boolean collect(String name) {
                if (!NodeStateUtils.isHidden(name) && !IGNORED_NODES.contains(name)) {
                    changed.add(name);
                }
                return true;
            }
        });
        for (String name : changed) {
            try {
                audit(definitionsPath, name, before, after);
            } catch (RuntimeException e) {
                // one broken history must not stop recording the changes of the other indexes
                LOG.warn("Failed to record the change of index definition {}", PathUtils.concat(definitionsPath, name), e);
            }
        }
        if (deletedHistoryAdded) {
            deletedHistoryAdded = false;
            NodeBuilder deleted = rootBuilder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(AUDIT_NODE_NAME)
                    .getChildNode(DELETED_NODE_NAME);
            if (deleted.exists()) {
                removeOldDeleted(deleted);
            }
        }
    }

    private void audit(String definitionsPath, String name, NodeState beforeDefinitions, NodeState afterDefinitions) {
        String indexPath = PathUtils.concat(definitionsPath, name);
        NodeState before = beforeDefinitions.getChildNode(name);
        NodeState after = afterDefinitions.getChildNode(name);
        if (!before.hasProperty(TYPE_PROPERTY_NAME) && !after.hasProperty(TYPE_PROPERTY_NAME)) {
            // not an index definition (e.g. node type definitions use the name oak:index)
            return;
        }
        NodeState visibleBefore = cloneDefinition(before);
        NodeState visibleAfter = cloneDefinition(after);
        boolean reindexRequested = after.getBoolean(REINDEX_PROPERTY_NAME) && !before.getBoolean(REINDEX_PROPERTY_NAME);
        Operation op;
        String diff;
        String diffBase = null;
        if (!before.exists()) {
            // a new version that is disabled is how an index is removed (it replaces the previous version)
            op = TYPE_DISABLED.equals(visibleAfter.getString(TYPE_PROPERTY_NAME)) ? Operation.DISABLED
                    : visibleAfter.hasProperty(MERGES_PROPERTY) ? Operation.MERGED : Operation.CREATED;
            // compare a new version of an index with the index it is based on, so that the
            // entry shows what the new version changes
            diffBase = getPredecessor(name, visibleAfter, beforeDefinitions, afterDefinitions);
            NodeState base = diffBase == null ? EMPTY_NODE : cloneDefinition(
                    getDefinition(diffBase, beforeDefinitions, afterDefinitions));
            diff = JsopDiff.diffToJsop(base, visibleAfter);
            if (diffBase != null) {
                diffBase = PathUtils.concat(definitionsPath, diffBase);
            }
        } else if (!after.exists()) {
            op = Operation.DELETED;
            // the definition is known from the earlier entries
            diff = "";
        } else {
            diff = JsopDiff.diffToJsop(visibleBefore, visibleAfter);
            if (diff.isEmpty() && !isImport()) {
                // only hidden content (index data) or internal state changed
                return;
            }
            op = getOperation(visibleBefore, visibleAfter, reindexRequested);
        }
        diff = StringUtils.abbreviate(diff, MAX_DIFF_LENGTH);
        String user = StringUtils.abbreviate(getUser(op, before, after), MAX_VALUE_LENGTH);
        String why = StringUtils.abbreviate(getReason(op, before, after), MAX_VALUE_LENGTH);
        String origin = commitInfo.getSessionId();
        String thread = StringUtils.abbreviate(this.thread != null ? this.thread : Thread.currentThread().getName(),
                MAX_VALUE_LENGTH);
        boolean flagReindexRequested = reindexRequested && op != Operation.REINDEX;
        NodeBuilder globalDefinitions = rootBuilder.getChildNode(INDEX_DEFINITIONS_NAME);
        if (!globalDefinitions.exists()) {
            // the global oak:index node was removed: nowhere to store the entry
            return;
        }
        NodeBuilder audit = globalDefinitions.child(AUDIT_NODE_NAME);
        long now = clock.millis();
        // the history of a removed index with the same path continues (also if the index was
        // created again while the audit was disabled)
        restoreDeletedHistory(audit, indexPath);
        NodeBuilder history = childBuilder(audit, indexPath);
        NodeBuilder entry = history.child(newEntryName(history, MINUTE_FORMAT.format(Instant.ofEpochMilli(now))));
        entry.setProperty(PROP_PATH, indexPath);
        entry.setProperty(PROP_OPERATION, op.name());
        entry.setProperty(PROP_USER, user);
        entry.setProperty(PROP_ORIGIN, origin);
        entry.setProperty(PROP_TIME, formatDate(now), Type.DATE);
        entry.setProperty(PROP_THREAD, thread);
        entry.setProperty(PROP_COMMIT_PATH, commitInfo.getPath());
        entry.setProperty(PROP_DIFF, diff);
        if (diffBase != null) {
            entry.setProperty(PROP_DIFF_BASE, diffBase);
        }
        if (why != null && !why.isEmpty()) {
            entry.setProperty(PROP_REASON, why);
        }
        if (flagReindexRequested) {
            entry.setProperty(PROP_REINDEX_REQUESTED, true);
        }
        removeOldEntries(history);
        if (op == Operation.DELETED) {
            moveToDeleted(audit, indexPath, now);
        }
    }

    /**
     * Returns the name of the index definition a new index definition is based on: the
     * product index it was merged from, or else the closest lower version of the same
     * index (for example {@code damAssetLucene-12} for {@code damAssetLucene-12-custom-1}).
     *
     * @return the name, or null if there is none
     */
    private static String getPredecessor(String name, NodeState definition,
            NodeState beforeDefinitions, NodeState afterDefinitions) {
        PropertyState merges = definition.getProperty(MERGES_PROPERTY);
        if (merges != null && merges.isArray()) {
            String firstExisting = null;
            for (String path : merges.getValue(Type.STRINGS)) {
                String source = PathUtils.getName(path);
                if (source.equals(name) || !getDefinition(source, beforeDefinitions, afterDefinitions).exists()) {
                    continue;
                }
                if (IndexName.parse(source).getCustomerVersion() == 0) {
                    return source;
                }
                if (firstExisting == null) {
                    firstExisting = source;
                }
            }
            if (firstExisting != null) {
                return firstExisting;
            }
        }
        IndexName indexName = IndexName.parse(name);
        IndexName predecessor = null;
        Set<String> candidates = new TreeSet<>();
        beforeDefinitions.getChildNodeNames().forEach(candidates::add);
        afterDefinitions.getChildNodeNames().forEach(candidates::add);
        for (String candidate : candidates) {
            if (NodeStateUtils.isHidden(candidate) || candidate.equals(name)
                    || !getDefinition(candidate, beforeDefinitions, afterDefinitions).hasProperty(TYPE_PROPERTY_NAME)) {
                continue;
            }
            IndexName n = IndexName.parse(candidate);
            if (n.getBaseName().equals(indexName.getBaseName()) && n.compareTo(indexName) < 0
                    && (predecessor == null || n.compareTo(predecessor) > 0)) {
                predecessor = n;
            }
        }
        return predecessor == null ? null : predecessor.getNodeName();
    }

    /**
     * Returns the index definition after the commit, or before the commit if it was
     * removed in the same commit (for example an older merged version).
     */
    private static NodeState getDefinition(String name, NodeState beforeDefinitions, NodeState afterDefinitions) {
        NodeState after = afterDefinitions.getChildNode(name);
        return after.exists() ? after : beforeDefinitions.getChildNode(name);
    }

    /**
     * Returns who changed the index definition: for a deletion or a disable done by a
     * tool, the tool tells it with a property ({@code deletedBy} set before the deletion,
     * {@code disabledBy} set in the same commit); otherwise the committing user.
     */
    private String getUser(Operation op, NodeState before, NodeState after) {
        String by = null;
        if (op == Operation.DELETED) {
            by = before.getString(DELETED_BY);
        } else if (op == Operation.DISABLED) {
            by = after.getString(DISABLED_BY);
            if (by != null && by.equals(before.getString(DISABLED_BY))) {
                // set by an earlier disable
                by = null;
            }
        }
        return by == null || by.isEmpty() ? commitInfo.getUserId() : by;
    }

    /**
     * Returns why the index definition was changed: the reason given in the commit info
     * (see {@link #commitInfo(String, String)}), else for a disable the
     * {@code disabledReason} set in the same commit; null if unknown.
     */
    private String getReason(Operation op, NodeState before, NodeState after) {
        if (reason != null) {
            return reason;
        }
        if (op == Operation.DISABLED) {
            String disabledReason = after.getString(DISABLED_REASON);
            if (disabledReason != null && !disabledReason.equals(before.getString(DISABLED_REASON))) {
                return disabledReason;
            }
        }
        return null;
    }

    private boolean isImport() {
        return ORIGIN_INDEX_IMPORTER.equals(commitInfo.getSessionId());
    }

    private Operation getOperation(NodeState before, NodeState after, boolean reindexRequested) {
        if (isImport()) {
            return Operation.REINDEX;
        }
        boolean disabledBefore = TYPE_DISABLED.equals(before.getString(TYPE_PROPERTY_NAME));
        boolean disabledAfter = TYPE_DISABLED.equals(after.getString(TYPE_PROPERTY_NAME));
        if (disabledAfter && !disabledBefore) {
            return Operation.DISABLED;
        }
        if (disabledBefore && !disabledAfter) {
            return Operation.ENABLED;
        }
        if (reindexRequested && withoutReindexFlag(before).equals(withoutReindexFlag(after))) {
            return Operation.REINDEX;
        }
        return Operation.CHANGED;
    }

    private static NodeState withoutReindexFlag(NodeState definition) {
        NodeBuilder builder = definition.builder();
        builder.removeProperty(REINDEX_PROPERTY_NAME);
        return builder.getNodeState();
    }

    /**
     * Moves the history of a removed index to the deleted indexes, merging it with
     * the history of a previously removed index with the same path.
     */
    private void moveToDeleted(NodeBuilder audit, String indexPath, long now) {
        NodeBuilder deleted = audit.child(DELETED_NODE_NAME);
        NodeBuilder target = childBuilder(deleted, indexPath);
        moveEntries(childBuilder(audit, indexPath), target);
        removeEmptyNodes(audit, indexPath);
        target.setProperty(PROP_DELETED_AT, formatDate(now), Type.DATE);
        // the number of deleted histories is limited once per commit (see audit)
        deletedHistoryAdded = true;
    }

    /**
     * Moves the history of a previously removed index with the same path back.
     */
    private void restoreDeletedHistory(NodeBuilder audit, String indexPath) {
        NodeBuilder deleted = audit.getChildNode(DELETED_NODE_NAME);
        NodeBuilder source = getBuilder(deleted, indexPath);
        if (!source.exists()) {
            return;
        }
        moveEntries(source, childBuilder(audit, indexPath));
        removeEmptyNodes(audit, "/" + DELETED_NODE_NAME + indexPath);
    }

    private void moveEntries(NodeBuilder source, NodeBuilder target) {
        for (String name : getEntryNames(source)) {
            String targetName = target.hasChildNode(name)
                    ? newEntryName(target, entryNameMatcher(name).group(1))
                    : name;
            target.setChildNode(targetName, source.getChildNode(name).getNodeState());
        }
        source.remove();
        removeOldEntries(target);
    }

    private void removeOldEntries(NodeBuilder history) {
        List<String> names = getEntryNames(history);
        for (String name : names.subList(0, Math.max(0, names.size() - maxEntries))) {
            history.getChildNode(name).remove();
        }
    }

    private void removeOldDeleted(NodeBuilder deleted) {
        List<Map.Entry<String, Long>> histories = new ArrayList<>();
        collectDeletedHistories(deleted.getNodeState(), "/", histories);
        if (histories.size() <= maxDeleted) {
            return;
        }
        histories.sort(Map.Entry.comparingByValue());
        for (Map.Entry<String, Long> history : histories.subList(0, histories.size() - maxDeleted)) {
            getBuilder(deleted, history.getKey()).remove();
            removeEmptyNodes(deleted, PathUtils.getParentPath(history.getKey()));
        }
    }

    private static void collectDeletedHistories(NodeState state, String path, List<Map.Entry<String, Long>> histories) {
        PropertyState deletedAt = state.getProperty(PROP_DELETED_AT);
        if (deletedAt != null) {
            // a history: its child nodes are entries
            histories.add(Map.entry(path, getDeletedAt(deletedAt)));
            return;
        }
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            if (!ENTRY_NAME.matcher(child.getName()).matches()) {
                collectDeletedHistories(child.getNodeState(), PathUtils.concat(path, child.getName()), histories);
            }
        }
    }

    /**
     * Returns when a history was deleted (milliseconds); an unreadable value counts as the
     * oldest (so that a broken history is removed first, and does not stop removing the
     * oldest histories).
     */
    private static long getDeletedAt(PropertyState deletedAt) {
        try {
            Calendar date = deletedAt.isArray() ? null : ISO8601.parse(deletedAt.getValue(Type.STRING));
            return date == null ? Long.MIN_VALUE : date.getTimeInMillis();
        } catch (RuntimeException e) {
            return Long.MIN_VALUE;
        }
    }

    private static String newEntryName(NodeBuilder history, String minute) {
        long sequence = 0;
        for (String name : history.getChildNodeNames()) {
            Matcher matcher = ENTRY_NAME.matcher(name);
            if (matcher.matches() && matcher.group(1).equals(minute)) {
                sequence = Math.max(sequence, Long.parseLong(matcher.group(2)) + 1);
            }
        }
        return minute + "." + sequence;
    }

    private static List<String> getEntryNames(NodeBuilder history) {
        List<String> names = new ArrayList<>();
        for (String name : history.getChildNodeNames()) {
            if (ENTRY_NAME.matcher(name).matches()) {
                names.add(name);
            }
        }
        names.sort(ENTRY_ORDER);
        return names;
    }

    static boolean isEntryName(String name) {
        return ENTRY_NAME.matcher(name).matches();
    }

    private static Matcher entryNameMatcher(String name) {
        Matcher matcher = ENTRY_NAME.matcher(name);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(name);
        }
        return matcher;
    }


    /**
     * Removes the node at the given path and its ancestors (up to, excluding, the
     * given base) if they have no child nodes and no properties.
     */
    private static void removeEmptyNodes(NodeBuilder base, String path) {
        while (!PathUtils.denotesRoot(path)) {
            NodeBuilder builder = getBuilder(base, path);
            if (builder.exists() && (builder.getChildNodeCount(1) > 0 || builder.getPropertyCount() > 0)) {
                return;
            }
            builder.remove();
            path = PathUtils.getParentPath(path);
        }
    }

    private static String formatDate(long millis) {
        return Conversions.convert(millis).toDate();
    }

    /**
     * Returns the visible part of the definition, without the internal state of an
     * out-of-band index import (temporary async lane, import state) and without
     * bookkeeping properties of the index implementation.
     */
    private static NodeState cloneDefinition(NodeState state) {
        NodeBuilder builder = EMPTY_NODE.builder();
        copyVisible(state, builder);
        if (AsyncLaneSwitcher.isLaneSwitched(builder)) {
            // also sets the refresh flag, which is ignored below
            AsyncLaneSwitcher.revertSwitch(builder, "");
        }
        builder.removeProperty(IndexImporter.INDEX_IMPORT_STATE_KEY);
        IGNORED_PROPERTIES.forEach(builder::removeProperty);
        // a reindex request is recorded as the operation or as reindexRequested
        PropertyState reindex = builder.getProperty(REINDEX_PROPERTY_NAME);
        if (reindex != null && !reindex.isArray() && !reindex.getValue(Type.BOOLEAN)) {
            builder.removeProperty(REINDEX_PROPERTY_NAME);
        }
        return builder.getNodeState();
    }

    /**
     * Copies the properties and child nodes, except the hidden ones.
     */
    static void copyVisible(NodeState state, NodeBuilder builder) {
        for (PropertyState property : state.getProperties()) {
            if (!NodeStateUtils.isHidden(property.getName())) {
                builder.setProperty(property);
            }
        }
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            if (!NodeStateUtils.isHidden(child.getName())) {
                copyVisible(child.getNodeState(), builder.child(child.getName()));
            }
        }
    }
}
