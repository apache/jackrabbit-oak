/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.CatchUpCapable.CATCH_UP_FROM_START;
import static org.apache.jackrabbit.oak.plugins.index.CatchUpCapable.CATCH_UP_TRACKING_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

/**
 * Runs per-target catch-up diffs after each successful lane commit.
 *
 * <p>For each index definition that has a {@code tracking} child node,
 * this runner resolves the checkpoint stored in each property, runs an
 * {@link EditorDiff} from that point to the current lane state, and
 * advances the property to the lane's after-checkpoint on success.
 * On failure the property is left unchanged so the next cycle retries.</p>
 *
 * <p>Each target is committed independently. A failure on one target
 * does not prevent other targets from being processed.</p>
 */
public class CatchUpRunner {

    private static final Logger LOG = LoggerFactory.getLogger(CatchUpRunner.class);

    private final NodeStore store;
    private final IndexEditorProvider provider;
    private final String laneName;

    public CatchUpRunner(@NotNull NodeStore store, @NotNull IndexEditorProvider provider) {
        this(store, provider, null);
    }

    public CatchUpRunner(@NotNull NodeStore store, @NotNull IndexEditorProvider provider, String laneName) {
        this.store = store;
        this.provider = provider;
        this.laneName = laneName;
    }

    /**
     * Runs catch-up for all pending targets across all index definitions.
     *
     * @param sourceRoot      the root state to read index definitions from (should be the latest committed state)
     * @param after           the lane's after-state (checkpoint state used as catch-up target)
     * @param afterCheckpoint the checkpoint string identifying {@code after}
     */
    public void run(@NotNull NodeState sourceRoot, @NotNull NodeState after, @NotNull String afterCheckpoint) {
        if (!(provider instanceof CatchUpCapable)) {
            return;
        }
        // No need to cast - we just use provider.getIndexEditor()

        NodeState oakIndex = sourceRoot.getChildNode("oak:index");
        LOG.info("CatchUpRunner.run() called - scanning indexes");
        int indexCount = 0;
        int targetCount = 0;
        for (String indexName : oakIndex.getChildNodeNames()) {
            NodeState indexDef = oakIndex.getChildNode(indexName);
            NodeState trackingNode = indexDef.getChildNode(CATCH_UP_TRACKING_NODE);
            if (!trackingNode.exists()) {
                continue;
            }
            if (!isOwnedByLane(indexDef)) {
                continue;
            }
            indexCount++;
            LOG.info("  Index #{}: {} has tracking node", indexCount, indexName);

            for (PropertyState prop : trackingNode.getProperties()) {
                String targetType = prop.getName();
                if (targetType.startsWith(":") || targetType.startsWith("jcr:")) {
                    continue; // skip Oak internal properties (e.g. :childOrder) and JCR properties (e.g. jcr:primaryType)
                }
                targetCount++;
                String trackingCheckpoint = prop.getValue(Type.STRING);
                LOG.info("    Target #{}: {}/{} = {}", targetCount, indexName, targetType, trackingCheckpoint);
                 runForTarget(sourceRoot, indexName, targetType,
                        trackingCheckpoint, after, afterCheckpoint);
            }
        }
        LOG.info("CatchUpRunner.run() completed - processed {} indexes, {} targets", indexCount, targetCount);
    }

    private void runForTarget(@NotNull NodeState sourceRoot,
                              @NotNull String indexName,
                              @NotNull String targetType,
                              @NotNull String trackingCheckpoint,
                              @NotNull NodeState after,
                              @NotNull String afterCheckpoint) {

        // Resolve before-state
        NodeState before;
        if (CATCH_UP_FROM_START.equals(trackingCheckpoint)) {
            before = MISSING_NODE;
            LOG.info("Catch-up: starting full traversal for {}/{}", indexName, targetType);
        } else {
            before = store.retrieve(trackingCheckpoint);
            if (before == null) {
                LOG.warn("Catch-up: checkpoint {} expired for {}/{}, falling back to full traversal",
                        trackingCheckpoint, indexName, targetType);
                before = MISSING_NODE;
            } else {
                LOG.debug("Catch-up: incremental diff for {}/{} from {}", indexName, targetType, trackingCheckpoint);
            }
        }

        // Create a builder from sourceRoot (which has the latest committed definition)
        // This ensures we read the same index definition state that was just committed
        NodeBuilder commitRootBuilder = sourceRoot.builder();
        NodeBuilder indexDefForReading = commitRootBuilder.child("oak:index").child(indexName);
        NodeBuilder indexDefForCommitting = indexDefForReading; // Same builder for reading and committing

        try {
            // Call getIndexEditor() with the targetType - same as normal indexing!
            // The provider will check if it handles this targetType via shouldWrite()
            // Create a proper ContextAwareCallback for providers that require it (like Lucene)
            String indexPath = "/oak:index/" + indexName;
            IndexUpdateCallback callback = new CatchUpCallback(indexPath);
            Editor editor = provider.getIndexEditor(targetType, indexDefForReading, after, callback);
            if (editor == null) {
                LOG.debug("Catch-up: no editor for {}/{}, skipping", indexName, targetType);
                return;
            }

            CommitFailedException error = EditorDiff.process(editor, before, after);
            if (error != null) {
                LOG.error("Catch-up: diff failed for {}/{}, will retry next cycle: {}",
                        indexName, targetType, error.getMessage());
                return; // do not commit — leave tracking property unchanged
            }

            // Advance tracking property to afterCheckpoint in the commit builder
            indexDefForCommitting.child(CATCH_UP_TRACKING_NODE)
                    .setProperty(targetType, afterCheckpoint);
            store.merge(commitRootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            LOG.info("Catch-up: advanced {}/{} to checkpoint {}", indexName, targetType, afterCheckpoint);

        } catch (CommitFailedException e) {
            LOG.error("Catch-up: failed for {}/{}, will retry next cycle",
                    indexName, targetType, e);
            // Tracking property left unchanged — retry next cycle
        }
    }

    private boolean isOwnedByLane(NodeState indexDef) {
        if (laneName == null) {
            return true;
        }
        PropertyState asyncProp = indexDef.getProperty(IndexConstants.ASYNC_PROPERTY_NAME);
        if (asyncProp == null) {
            return false;
        }
        for (String value : asyncProp.getValue(Type.STRINGS)) {
            if (laneName.equals(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Simple ContextAwareCallback implementation for catch-up indexing.
     * Provides minimal context information required by index providers like Lucene.
     */
    private static class CatchUpCallback implements ContextAwareCallback, IndexingContext {
        private final String indexPath;

        CatchUpCallback(String indexPath) {
            this.indexPath = indexPath;
        }

        @Override
        public void indexUpdate() {
            // No-op for catch-up
        }

        @Override
        public IndexingContext getIndexingContext() {
            return this;
        }

        @Override
        public String getIndexPath() {
            return indexPath;
        }

        @Override
        public CommitInfo getCommitInfo() {
            return CommitInfo.EMPTY;
        }

        @Override
        public boolean isReindexing() {
            return false; // Catch-up is not a full reindex
        }

        @Override
        public boolean isAsync() {
            return true; // Catch-up runs in async indexer
        }

        @Override
        public void registerIndexCommitCallback(IndexCommitCallback callback) {
            // No-op for catch-up - we don't need commit callbacks
        }

        @Override
        public void indexUpdateFailed(Exception e) {
            // No-op for catch-up - errors are handled by CatchUpRunner
        }
    }
}
