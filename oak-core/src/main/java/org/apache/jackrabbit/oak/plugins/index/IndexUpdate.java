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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_REINDEX_VALUE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_PATH;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.commit.CompositeEditor.compose;
import static org.apache.jackrabbit.oak.spi.commit.EditorDiff.process;
import static org.apache.jackrabbit.oak.spi.commit.VisibleEditor.wrap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.ProgressNotificationEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexUpdate implements Editor {

    private static final Logger log = LoggerFactory.getLogger(IndexUpdate.class);

    /**
     * <p>
     * The value of this flag determines the behavior of the IndexUpdate when
     * dealing with {@code reindex} flags.
     * </p>
     * <p>
     * If {@code false} (default value), the indexer will start reindexing
     * immediately in the current thread, blocking a commit until this operation
     * is done.
     * </p>
     * <p>
     * If {@code true}, the indexer will ignore the flag, therefore ignoring any
     * reindex requests.
     * </p>
     * <p>
     * This is only provided as a support tool (see OAK-3505) so it should be
     * used with extreme caution!
     * </p>
     */
    private static final boolean IGNORE_REINDEX_FLAGS = Boolean
            .getBoolean("oak.indexUpdate.ignoreReindexFlags");

    static {
        if (IGNORE_REINDEX_FLAGS) {
            log.warn("Reindexing is disabled by configuration. This value is configurable via the 'oak.indexUpdate.ignoreReindexFlags' system property.");
        }
    }

    private final IndexUpdateRootState rootState;

    private final NodeBuilder builder;

    /** Parent updater, or {@code null} if this is the root updater. */
    private final IndexUpdate parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /**
     * Editors for indexes that will be normally updated.
     */
    private final List<Editor> editors = newArrayList();

    /**
     * Editors for indexes that need to be re-indexed.
     */
    private final Map<String, Editor> reindex = new HashMap<String, Editor>();

    private MissingIndexProviderStrategy missingProvider = new MissingIndexProviderStrategy();

    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.rootState = new IndexUpdateRootState(provider, async, root, updateCallback);
        this.builder = checkNotNull(builder);
    }

    private IndexUpdate(IndexUpdate parent, String name) {
        this.parent = checkNotNull(parent);
        this.name = name;
        this.rootState = parent.rootState;
        this.builder = parent.builder.getChildNode(checkNotNull(name));
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        collectIndexEditors(builder.getChildNode(INDEX_DEFINITIONS_NAME), before);

        if (!reindex.isEmpty()) {
            log.info("Reindexing will be performed for following indexes: {}",
                    reindex.keySet());
            rootState.reindexedIndexes.addAll(reindex.keySet());
        }

        // no-op when reindex is empty
        CommitFailedException exception = process(
                wrap(wrapProgress(compose(reindex.values()), "Reindexing")), MISSING_NODE, after);
        if (exception != null) {
            throw exception;
        }

        for (Editor editor : editors) {
            editor.enter(before, after);
        }
    }

    public boolean isReindexingPerformed(){
        return !getReindexStats().isEmpty();
    }

    public List<String> getReindexStats(){
        return rootState.getReindexStats();
    }

    private boolean shouldReindex(NodeBuilder definition, NodeState before,
            String name) {
        PropertyState ps = definition.getProperty(REINDEX_PROPERTY_NAME);
        if (ps != null && ps.getValue(BOOLEAN)) {
            return !IGNORE_REINDEX_FLAGS;
        }
        // reindex in the case this is a new node, even though the reindex flag
        // might be set to 'false' (possible via content import)
        boolean result = !before.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name);
        if (result) {
            log.info("Found a new index node [{}]. Reindexing is requested",
                    name);
        }
        return result;
    }

    private void collectIndexEditors(NodeBuilder definitions,
            NodeState before) throws CommitFailedException {
        for (String name : definitions.getChildNodeNames()) {
            NodeBuilder definition = definitions.getChildNode(name);
            if (isIncluded(rootState.async, definition)) {
                String type = definition.getString(TYPE_PROPERTY_NAME);
                if (type == null) {
                    // probably not an index def
                    continue;
                }
                manageIndexPath(definition, name);
                boolean shouldReindex = shouldReindex(definition,
                        before, name);
                String indexPath = getIndexPath(getPath(), name);
                Editor editor = rootState.provider.getIndexEditor(type, definition, rootState.root,
                        rootState.newCallback(indexPath, shouldReindex));
                if (editor == null) {
                    missingProvider.onMissingIndex(type, definition, indexPath);
                } else if (shouldReindex) {
                    if (definition.getBoolean(REINDEX_ASYNC_PROPERTY_NAME)
                            && definition.getString(ASYNC_PROPERTY_NAME) == null) {
                        // switch index to an async update mode
                        definition.setProperty(ASYNC_PROPERTY_NAME,
                                ASYNC_REINDEX_VALUE);
                    } else {
                        definition.setProperty(REINDEX_PROPERTY_NAME, false);
                        incrementReIndexCount(definition);
                        // as we don't know the index content node name
                        // beforehand, we'll remove all child nodes
                        for (String rm : definition.getChildNodeNames()) {
                            if (NodeStateUtils.isHidden(rm)) {
                                definition.getChildNode(rm).remove();
                            }
                        }
                        reindex.put(concat(getPath(), INDEX_DEFINITIONS_NAME, name), editor);
                    }
                } else {
                    editors.add(editor);
                }
            }
        }
    }

    static boolean isIncluded(String asyncRef, NodeBuilder definition) {
        if (definition.hasProperty(ASYNC_PROPERTY_NAME)) {
            PropertyState p = definition.getProperty(ASYNC_PROPERTY_NAME);
            List<String> opt = newArrayList(p.getValue(Type.STRINGS));
            if (asyncRef == null) {
                // sync index job, accept synonyms
                return opt.contains("") || opt.contains("sync");
            } else {
                return opt.contains(asyncRef);
            }
        } else {
            return asyncRef == null;
        }
    }

    private void manageIndexPath(NodeBuilder definition, String name) {
        String path = definition.getString(INDEX_PATH);
        if (path == null){
            definition.setProperty(INDEX_PATH, PathUtils.concat(getPath(), INDEX_DEFINITIONS_NAME, name));
        }
    }

    private void incrementReIndexCount(NodeBuilder definition) {
        long count = 0;
        if(definition.hasProperty(REINDEX_COUNT)){
            count = definition.getProperty(REINDEX_COUNT).getValue(Type.LONG);
        }
        definition.setProperty(REINDEX_COUNT, count + 1);
    }

    /**
     * Returns the path of this node, building it lazily when first requested.
     */
    private String getPath() {
        if (path == null) {
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        for (Editor editor : editors) {
            editor.leave(before, after);
        }

        if (parent == null){
            if (rootState.isReindexingPerformed()){
                log.info(rootState.getReport());
            } else if (log.isDebugEnabled() && rootState.somethingIndexed()){
                log.debug(rootState.getReport());
            }
        }
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        for (Editor editor : editors) {
            editor.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        for (Editor editor : editors) {
            editor.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        for (Editor editor : editors) {
            editor.propertyDeleted(before);
        }
    }

    @Override @Nonnull
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        List<Editor> children = newArrayListWithCapacity(1 + editors.size());
        children.add(new IndexUpdate(this, name));
        for (Editor editor : editors) {
            Editor child = editor.childNodeAdded(name, after);
            if (child != null) {
                children.add(child);
            }
        }
        return compose(children);
    }

    @Override @Nonnull
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        List<Editor> children = newArrayListWithCapacity(1 + editors.size());
        children.add(new IndexUpdate(this, name));
        for (Editor editor : editors) {
            Editor child = editor.childNodeChanged(name, before, after);
            if (child != null) {
                children.add(child);
            }
        }
        return compose(children);
    }

    @Override @CheckForNull
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        List<Editor> children = newArrayListWithCapacity(editors.size());
        for (Editor editor : editors) {
            Editor child = editor.childNodeDeleted(name, before);
            if (child != null) {
                children.add(child);
            }
        }
        return compose(children);
    }

    protected Set<String> getReindexedDefinitions() {
        return reindex.keySet();
    }

    private static String getIndexPath(String path, String indexName) {
        if (PathUtils.denotesRoot(path)) {
            return "/" + INDEX_DEFINITIONS_NAME + "/" + indexName;
        }
        return path + "/" + INDEX_DEFINITIONS_NAME + "/" + indexName;
    }

    private static Editor wrapProgress(Editor editor, String message){
        return ProgressNotificationEditor.wrap(editor, log, message);
    }

    public static class MissingIndexProviderStrategy {

        /**
         * The value of this flag determines the behavior of
         * {@link #onMissingIndex(String, NodeBuilder, String)}. If
         * {@code false} (default value), the method will set the
         * {@code reindex} flag to true and log a warning. if {@code true}, the
         * method will throw a {@link CommitFailedException} failing the commit.
         */
        private boolean failOnMissingIndexProvider = Boolean
                .getBoolean("oak.indexUpdate.failOnMissingIndexProvider");

        private final Set<String> ignore = newHashSet("disabled");

        public void onMissingIndex(String type, NodeBuilder definition, String indexPath)
                throws CommitFailedException {
            if (isDisabled(type)) {
                return;
            }
            // trigger reindexing when an indexer becomes available
            PropertyState ps = definition.getProperty(REINDEX_PROPERTY_NAME);
            if (ps != null && ps.getValue(BOOLEAN)) {
                // already true, skip the update
                return;
            }

            if (failOnMissingIndexProvider) {
                throw new CommitFailedException("IndexUpdate", 1,
                        "Missing index provider detected for type [" + type
                                + "] on index [" + indexPath + "]");
            } else {
                log.warn(
                        "Missing index provider of type [{}], requesting reindex on [{}]",
                        type, indexPath);
                definition.setProperty(REINDEX_PROPERTY_NAME, true);
            }
        }

        boolean isDisabled(String type) {
            return ignore.contains(type);
        }

        void setFailOnMissingIndexProvider(boolean failOnMissingIndexProvider) {
            this.failOnMissingIndexProvider = failOnMissingIndexProvider;
        }
    }

    public IndexUpdate withMissingProviderStrategy(
            MissingIndexProviderStrategy missingProvider) {
        this.missingProvider = missingProvider;
        return this;
    }

    private static final class IndexUpdateRootState {
        final IndexEditorProvider provider;
        final String async;
        final NodeState root;
        /**
         * Callback for the update events of the indexing job
         */
        final IndexUpdateCallback updateCallback;
        final Set<String> reindexedIndexes = Sets.newHashSet();
        final Map<String, CountingCallback> callbacks = Maps.newHashMap();

        private IndexUpdateRootState(IndexEditorProvider provider, String async, NodeState root,
                                     IndexUpdateCallback updateCallback) {
            this.provider = checkNotNull(provider);
            this.async = async;
            this.root = checkNotNull(root);
            this.updateCallback = checkNotNull(updateCallback);
        }

        public IndexUpdateCallback newCallback(String indexPath, boolean reindex) {
            CountingCallback cb = new CountingCallback(indexPath, reindex);
            callbacks.put(cb.indexName, cb);
            return cb;
        }

        public String getReport() {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println("Indexing report");
            for (CountingCallback cb : callbacks.values()) {
                if (!log.isDebugEnabled() && !cb.reindex) {
                    continue;
                }
                if (cb.count > 0) {
                    pw.printf("    - %s%n", cb);
                }
            }
            return sw.toString();
        }

        public List<String> getReindexStats(){
            List<String> stats = Lists.newArrayList();
            for (CountingCallback cb : callbacks.values()){
                if (cb.reindex) {
                    stats.add(cb.toString());
                }
            }
            return stats;
        }

        public boolean somethingIndexed() {
            for (CountingCallback cb : callbacks.values()) {
                if (cb.count > 0){
                    return true;
                }
            }
            return false;
        }

        public boolean isReindexingPerformed(){
            return !reindexedIndexes.isEmpty();
        }

        private class CountingCallback implements IndexUpdateCallback {
            final String indexName;
            final boolean reindex;
            final Stopwatch watch = Stopwatch.createStarted();
            int count;

            private CountingCallback(String indexName, boolean reindex) {
                this.indexName = indexName;
                this.reindex = reindex;
            }

            @Override
            public void indexUpdate() throws CommitFailedException {
                count++;
                if (count % 10000 == 0){
                    log.info("{} => Indexed {} nodes in {} ...", indexName, count, watch);
                    watch.reset().start();
                }
                updateCallback.indexUpdate();
            }

            @Override
            public String toString() {
                String reindexMarker = reindex ? "*" : "";
                return indexName + reindexMarker + "(" + count + ")";
            }
        }
    }

}
