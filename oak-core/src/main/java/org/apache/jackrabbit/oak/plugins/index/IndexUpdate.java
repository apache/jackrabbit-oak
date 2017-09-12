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
import static com.google.common.collect.Sets.newIdentityHashSet;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_REINDEX_VALUE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_NRT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_SYNC;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.commit.CompositeEditor.compose;
import static org.apache.jackrabbit.oak.spi.commit.EditorDiff.process;
import static org.apache.jackrabbit.oak.spi.commit.VisibleEditor.wrap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback.IndexProgress;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback.PathSource;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.NodeCountEstimator;
import org.apache.jackrabbit.oak.plugins.index.progress.TraversalRateEstimator;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexUpdate implements Editor, PathSource {

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
    static final boolean IGNORE_REINDEX_FLAGS = Boolean
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


    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback) {
        this(provider, async, root, builder, updateCallback, CommitInfo.EMPTY);
    }

    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback, CommitInfo commitInfo) {
        this(provider, async, root, builder, updateCallback, NodeTraversalCallback.NOOP, commitInfo, CorruptIndexHandler.NOOP);
    }

    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback, NodeTraversalCallback traversalCallback,
            CommitInfo commitInfo, CorruptIndexHandler corruptIndexHandler) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.rootState = new IndexUpdateRootState(provider, async, root, updateCallback, traversalCallback, commitInfo, corruptIndexHandler);
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
        rootState.nodeRead(this);
        collectIndexEditors(builder.getChildNode(INDEX_DEFINITIONS_NAME), before);

        if (!reindex.isEmpty()) {
            log.info("Reindexing will be performed for following indexes: {}",
                    reindex.keySet());
            rootState.progressReporter.reindexingTraversalStart(getPath());
        }

        // no-op when reindex is empty
        CommitFailedException exception = process(
                wrap(wrapProgress(compose(reindex.values()))), MISSING_NODE, after);
        rootState.progressReporter.reindexingTraversalEnd();
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
        return rootState.progressReporter.getReindexStats();
    }

    public Set<String> getUpdatedIndexPaths(){
        return rootState.progressReporter.getUpdatedIndexPaths();
    }

    public void setTraversalRateEstimator(TraversalRateEstimator estimator){
        rootState.progressReporter.setTraversalRateEstimator(estimator);
    }

    public void setNodeCountEstimator(NodeCountEstimator nodeCountEstimator){
        rootState.progressReporter.setNodeCountEstimator(nodeCountEstimator);
    }

    public String getIndexingStats(){
        return rootState.getIndexingStats();
    }

    public void setIgnoreReindexFlags(boolean ignoreReindexFlag){
        rootState.setIgnoreReindexFlags(ignoreReindexFlag);
    }

    private boolean shouldReindex(NodeBuilder definition, NodeState before,
            String name) {
        //Async indexes are not considered for reindexing for sync indexing
        if (!isMatchingIndexMode(definition)){
            return false;
        }

        PropertyState ps = definition.getProperty(REINDEX_PROPERTY_NAME);
        if (ps != null && ps.getValue(BOOLEAN)) {
            return !rootState.ignoreReindexFlags;
        }
        // reindex in the case this is a new node, even though the reindex flag
        // might be set to 'false' (possible via content import).
        // However if its already indexed i.e. has some hidden nodes (containing hidden data)
        // then no need to reindex
        boolean result = !before.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name)
                && !hasAnyHiddenNodes(definition);
        if (result) {
            log.info("Found a new index node [{}]. Reindexing is requested",
                    name);
        }
        return result;
    }

    private static boolean hasAnyHiddenNodes(NodeBuilder builder){
        for (String name : builder.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(name)){
                return true;
            }
        }
        return false;
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

                boolean shouldReindex = shouldReindex(definition, before, name);
                String indexPath = getIndexPath(getPath(), name);
                if (definition.hasProperty(IndexConstants.CORRUPT_PROPERTY_NAME) && !shouldReindex){
                    String corruptSince = definition.getProperty(IndexConstants.CORRUPT_PROPERTY_NAME).getValue(Type.DATE);
                    rootState.corruptIndexHandler.skippingCorruptIndex(rootState.async, indexPath, ISO8601.parse(corruptSince));
                    continue;
                }

                Editor editor = rootState.provider.getIndexEditor(type, definition, rootState.root,
                        rootState.newCallback(indexPath, shouldReindex, getEstimatedCount(definition)));
                if (editor == null) {
                    rootState.missingProvider.onMissingIndex(type, definition, indexPath);
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

                        clearCorruptFlag(definition, indexPath);
                        reindex.put(concat(getPath(), INDEX_DEFINITIONS_NAME, name), editor);
                    }
                } else {
                    editors.add(editor);
                }
            }
        }
    }

    private long getEstimatedCount(NodeBuilder indexDefinition) {
        //TODO Implement the estimate
        return -1;
    }

    static boolean isIncluded(String asyncRef, NodeBuilder definition) {
        if (definition.hasProperty(ASYNC_PROPERTY_NAME)) {
            PropertyState p = definition.getProperty(ASYNC_PROPERTY_NAME);
            Iterable<String> opt = p.getValue(Type.STRINGS);
            if (asyncRef == null) {
                // sync index job, accept synonyms
                return Iterables.contains(opt, INDEXING_MODE_NRT) || Iterables.contains(opt, INDEXING_MODE_SYNC);
            } else {
                return Iterables.contains(opt, asyncRef);
            }
        } else {
            return asyncRef == null;
        }
    }

    /**
     * Determines if the current indexing mode matches with the IndexUpdate mode.
     * For this match it only considers indexes either as
     * <ul>
     *     <li>sync - Index definition does not have async property defined</li>
     *     <li>async - Index definition has async property defined. It does not matter what its value is</li>
     * </ul>
     *
     * <p>Same applies for IndexUpdate also.
     *
     * <p>Note that this differs from #isIncluded which also considers the value of <code>async</code>
     * property to determine if the index should be selected for current IndexUpdate run.
     */
    private boolean isMatchingIndexMode(NodeBuilder definition){
        boolean async = definition.hasProperty(ASYNC_PROPERTY_NAME);
        //Either
        // 1. async index and async index update
        // 2. non async i.e. sync index and sync index update
        return async == rootState.isAsync();
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
    @Override
    public String getPath() {
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
            rootState.progressReporter.logReport();
        }
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        rootState.propertyChanged(after.getName());
        for (Editor editor : editors) {
            editor.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        rootState.propertyChanged(before.getName());
        for (Editor editor : editors) {
            editor.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        rootState.propertyChanged(before.getName());
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

    public void commitProgress(IndexProgress indexProgress) {
        rootState.commitProgress(indexProgress);
    }

    protected Set<String> getReindexedDefinitions() {
        return rootState.progressReporter.getReindexedIndexPaths();
    }

    private void clearCorruptFlag(NodeBuilder definition, String indexPath) {
        PropertyState corrupt = definition.getProperty(IndexConstants.CORRUPT_PROPERTY_NAME);
        //Remove any corrupt property
        if (corrupt != null) {
            definition.removeProperty(IndexConstants.CORRUPT_PROPERTY_NAME);
            log.info("Removing corrupt flag from index [{}] which has been marked " +
                    "as corrupt since [{}]", indexPath, corrupt.getValue(Type.DATE));
        }
    }

    private static String getIndexPath(String path, String indexName) {
        if (PathUtils.denotesRoot(path)) {
            return "/" + INDEX_DEFINITIONS_NAME + "/" + indexName;
        }
        return path + "/" + INDEX_DEFINITIONS_NAME + "/" + indexName;
    }

    private Editor wrapProgress(Editor editor){
        return rootState.progressReporter.wrapProgress(editor);
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

        private final Set<String> ignore = newHashSet("disabled", "ordered");

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
        rootState.setMissingProvider(missingProvider);
        return this;
    }

    private static final class IndexUpdateRootState {
        final IndexEditorProvider provider;
        final String async;
        final NodeState root;
        final CommitInfo commitInfo;
        private boolean ignoreReindexFlags = IGNORE_REINDEX_FLAGS;
        final Set<IndexCommitCallback> indexCommitCallbacks = newIdentityHashSet();
        final CorruptIndexHandler corruptIndexHandler;
        final IndexingProgressReporter progressReporter;
        private int changedNodeCount;
        private int changedPropertyCount;
        private MissingIndexProviderStrategy missingProvider = new MissingIndexProviderStrategy();

        private IndexUpdateRootState(IndexEditorProvider provider, String async, NodeState root,
                                     IndexUpdateCallback updateCallback, NodeTraversalCallback traversalCallback,
                                     CommitInfo commitInfo, CorruptIndexHandler corruptIndexHandler) {
            this.provider = checkNotNull(provider);
            this.async = async;
            this.root = checkNotNull(root);
            this.commitInfo = commitInfo;
            this.corruptIndexHandler = corruptIndexHandler;
            this.progressReporter = new IndexingProgressReporter(updateCallback, traversalCallback);
        }

        public IndexUpdateCallback newCallback(String indexPath, boolean reindex, long estimatedCount) {
            progressReporter.registerIndex(indexPath, reindex, estimatedCount);
            return new ReportingCallback(indexPath, reindex);
        }

        public boolean isAsync(){
            return async != null;
        }

        public void nodeRead(PathSource pathSource) throws CommitFailedException {
            changedNodeCount++;
            progressReporter.traversedNode(pathSource);
        }

        public void propertyChanged(String name){
            changedPropertyCount++;
        }

        public String getIndexingStats() {
            return String.format("changedNodeCount %d, changedPropertyCount %d",
                    changedNodeCount, changedPropertyCount);
        }

        public void setMissingProvider(MissingIndexProviderStrategy missingProvider) {
            this.missingProvider = missingProvider;
        }

        void setIgnoreReindexFlags(boolean ignoreReindexFlags) {
            this.ignoreReindexFlags = ignoreReindexFlags;
        }

        void registerIndexCommitCallbackInternal(IndexCommitCallback callback) {
            indexCommitCallbacks.add(callback);
        }

        public void commitProgress(IndexProgress indexProgress) {
            for (IndexCommitCallback icc : indexCommitCallbacks) {
                try {
                    icc.commitProgress(indexProgress);
                } catch (Exception e) {
                    log.warn("Commit progress callback threw an exception. Saving ourselves.", e);
                }
            }
        }

        private class ReportingCallback implements ContextAwareCallback, IndexingContext {
            final String indexPath;
            final boolean reindex;

            public ReportingCallback(String indexPath, boolean reindex) {
                this.indexPath = indexPath;
                this.reindex = reindex;
            }

            @Override
            public void indexUpdate() throws CommitFailedException {
               progressReporter.indexUpdate(indexPath);
            }

            //~------------------------------< ContextAwareCallback >

            @Override
            public IndexingContext getIndexingContext() {
                return this;
            }

            //~--------------------------------< IndexingContext >

            @Override
            public String getIndexPath() {
                return indexPath;
            }

            @Override
            public CommitInfo getCommitInfo() {
                return commitInfo;
            }

            @Override
            public boolean isReindexing() {
                return reindex;
            }

            @Override
            public boolean isAsync() {
                return IndexUpdateRootState.this.isAsync();
            }

            @Override
            public void indexUpdateFailed(Exception e) {
                corruptIndexHandler.indexUpdateFailed(async, indexPath, e);
            }

            @Override
            public void registerIndexCommitCallback(IndexCommitCallback callback) {
                registerIndexCommitCallbackInternal(callback);
            }
        }
    }

}
