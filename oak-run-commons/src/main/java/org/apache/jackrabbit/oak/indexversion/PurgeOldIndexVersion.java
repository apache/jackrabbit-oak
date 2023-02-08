/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.indexversion;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ORIGINAL_TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

public abstract class PurgeOldIndexVersion implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeOldIndexVersion.class);

    protected final Closer closer = Closer.create();

    /**
     * Execute purging index based on the index version naming and last time index time. This will purge base index.
     *
     * @param nodeStore             the node store
     * @param isReadWriteRepository bool to indicate if it's read write repository, if yes, the purge index will not execute
     * @param purgeThresholdMillis  the threshold of time length since last time index time to determine, will purge if exceed that
     * @param indexPaths            the index path or parent path
     *
     * @throws IOException
     * @throws CommitFailedException
     */
    public void execute(NodeStore nodeStore, boolean isReadWriteRepository, long purgeThresholdMillis, List<String> indexPaths) throws
            IOException, CommitFailedException {
        execute(nodeStore, isReadWriteRepository, purgeThresholdMillis, indexPaths, true);
    }
    /**
     * Execute purging index based on the index version naming and last time index time
     *
     * @param nodeStore             the node store
     * @param isReadWriteRepository bool to indicate if it's read write repository, if yes, the purge index will not execute
     * @param purgeThresholdMillis  the threshold of time length since last time index time to determine, will purge if exceed that
     * @param indexPaths            the index path or parent path
     * @param shouldPurgeBaseIndex  If set to true, will apply purge operations on active base index i.e. DELETE or DELETE_HIDDEN_AND_DISABLE
     *
     * @throws IOException
     * @throws CommitFailedException
     */
    public void execute(NodeStore nodeStore, boolean isReadWriteRepository, long purgeThresholdMillis, List<String> indexPaths, boolean shouldPurgeBaseIndex) throws
            IOException, CommitFailedException {

        List<IndexVersionOperation> purgeIndexList = getPurgeIndexes(nodeStore, purgeThresholdMillis, indexPaths, shouldPurgeBaseIndex);
        if (!purgeIndexList.isEmpty()) {
            if (isReadWriteRepository) {
                LOG.info("Found indexes for purging: '{}'", purgeIndexList);
                long purgeStart = System.currentTimeMillis();
                purgeOldIndexVersion(nodeStore, purgeIndexList);
                LOG.info("Index purging done, took '{}' ms", (System.currentTimeMillis() - purgeStart));
            } else {
                LOG.info("Repository is opened in read-only mode, the purging indexes for '{}' are: {}", indexPaths, purgeIndexList);
            }
        } else {
            LOG.info("No indexes are found to be purged");
        }
    }

    public List<IndexVersionOperation> getPurgeIndexes(NodeStore nodeStore, long purgeThresholdMillis, List<String> indexPaths, boolean shouldPurgeBaseIndex) throws IOException, CommitFailedException {
        List<IndexVersionOperation> purgeIndexList = new ArrayList<>();
        LOG.info("Getting indexes to purge over index paths '{}'", indexPaths);
        List<String> sanitisedIndexPaths = sanitiseUserIndexPaths(indexPaths);
        Set<String> indexPathSet = filterIndexPaths(nodeStore, getRepositoryIndexPaths(nodeStore), sanitisedIndexPaths);
        Map<String, Set<String>> segregateIndexes = segregateIndexes(indexPathSet);
        for (Map.Entry<String, Set<String>> entry : segregateIndexes.entrySet()) {
            String baseIndexPath = entry.getKey();
            String parentPath = PathUtils.getParentPath(baseIndexPath);
            List<IndexName> indexNameObjectList = getIndexNameObjectList(entry.getValue());
            LOG.info("Validate purge index over base of '{}', which includes: '{}'", baseIndexPath, indexNameObjectList);
            List<IndexVersionOperation> toDeleteIndexNameObjectList = IndexVersionOperation.generateIndexVersionOperationList(
                    nodeStore.getRoot(), parentPath, indexNameObjectList, purgeThresholdMillis, shouldPurgeBaseIndex, returnIgnoreIsIndexActiveCheck());
            toDeleteIndexNameObjectList.removeIf(item -> (item.getOperation() == IndexVersionOperation.Operation.NOOP));
            if (!toDeleteIndexNameObjectList.isEmpty()) {
                LOG.info("Found some index need to be purged over base'{}': '{}'", baseIndexPath, toDeleteIndexNameObjectList);
                purgeIndexList.addAll(toDeleteIndexNameObjectList);
            } else {
                LOG.info("No index found to be purged over base: '{}'", baseIndexPath);
            }
        }
        return purgeIndexList;
    }

    // Purge operations will also be performed on base index i.e. DELETE or DELETE_HIDDEN_AND_DISABLE
    public List<IndexVersionOperation> getPurgeIndexes(NodeStore nodeStore, long purgeThresholdMillis, List<String> indexPaths) throws IOException, CommitFailedException {
        return getPurgeIndexes(nodeStore, purgeThresholdMillis, indexPaths, true);
    }

    /**
     * @param userIndexPaths indexpaths provided by user
     * @return a list of Indexpaths having baseIndexpaths or path till oak:index
     * @throws IllegalArgumentException if the paths provided are not till oak:index or till index
     */
    private List<String> sanitiseUserIndexPaths(List<String> userIndexPaths) {
        List<String> sanitisedUserIndexPaths = new ArrayList<>();
        for (String userIndexPath : userIndexPaths) {
            if (PathUtils.getName(userIndexPath).equals(PurgeOldVersionUtils.OAK_INDEX)) {
                sanitisedUserIndexPaths.add(userIndexPath);
            } else if (PathUtils.getName(PathUtils.getParentPath(userIndexPath)).equals(PurgeOldVersionUtils.OAK_INDEX)) {
                sanitisedUserIndexPaths.add(IndexName.parse(userIndexPath).getBaseName());
            } else {
                throw new IllegalArgumentException(userIndexPath + " indexpath is not valid");
            }
        }
        return sanitisedUserIndexPaths;
    }

    /**
     * @param indexPathSet
     * @return a map with baseIndexName as key and a set of indexpaths having same baseIndexName
     */
    private Map<String, Set<String>> segregateIndexes(Set<String> indexPathSet) {
        Map<String, Set<String>> segregatedIndexes = new HashMap<>();
        for (String path : indexPathSet) {
            String baseIndexPath = IndexName.parse(path).getBaseName();
            Set<String> indexPaths = segregatedIndexes.get(baseIndexPath);
            if (indexPaths == null) {
                indexPaths = new HashSet<>();
            }
            indexPaths.add(path);
            segregatedIndexes.put(baseIndexPath, indexPaths);
        }
        return segregatedIndexes;
    }

    private Iterable<String> getRepositoryIndexPaths(NodeStore store) throws CommitFailedException, IOException {
        IndexPathService indexPathService = new IndexPathServiceImpl(store);
        return indexPathService.getIndexPaths();
    }


    /**
     * @param repositoryIndexPaths: list of indexpaths retrieved from  index service
     * @param commandlineIndexPaths indexpaths provided by user
     * @return returns set of indexpaths which are to be considered for purging
     */
    private Set<String> filterIndexPaths(NodeStore store, Iterable<String> repositoryIndexPaths, List<String> commandlineIndexPaths) {
        Set<String> filteredIndexPaths = new HashSet<>();
        for (String commandlineIndexPath : commandlineIndexPaths) {
            for (String repositoryIndexPath : repositoryIndexPaths) {
                if (PurgeOldVersionUtils.isIndexChildNode(commandlineIndexPath, repositoryIndexPath)
                        || PurgeOldVersionUtils.isBaseIndexEqual(commandlineIndexPath, repositoryIndexPath)) {
                    NodeState root = store.getRoot();
                    NodeBuilder rootBuilder = root.builder();
                    NodeBuilder nodeBuilder = PurgeOldVersionUtils.getNode(rootBuilder, repositoryIndexPath);
                    if (nodeBuilder.exists()) {
                        String type = nodeBuilder.getProperty(TYPE_PROPERTY_NAME).getValue(Type.STRING);
                        // If type = disabled. There can be 2 cases -
                        // 1. It was marked as disabled by this code , in which case the original type would be moved to orig_type property
                        // 2. It was marked disabled by someone else.


                        // Skip the run if (type = disabled and orig_type is present and not equal to PurgeOldIndexVersion's impl's  index type)
                        // OR
                        // (type != disabled and type is not equal to PurgeOldIndexVersion's impl's  index type)
                        if ((TYPE_DISABLED.equals(type) && nodeBuilder.getProperty(ORIGINAL_TYPE_PROPERTY_NAME) != null && !getIndexType().equals(nodeBuilder.getProperty(ORIGINAL_TYPE_PROPERTY_NAME).getValue(Type.STRING))) ||
                                (!TYPE_DISABLED.equals(type) && !getIndexType().equals(type))) {
                            continue;
                        }

                        // If type = disabled and orig_type is not set - this means someone manually marked the index as disabled
                        // We can't determine the original index implementation in this case - so we let it run for whichever PurgeOldIndexVersion's impl calls it first
                        // Two scenarios to note here -
                        // 1. ElasticPurgeOldIndexVersion calls this for a lucene index (deletes the index from oak repo - which we want)
                        // and then in postDeleteOp tries to delete a non existent remote elastic index - that particular call will simply fail.
                        // 2. LucenePurgeOldIndexVersion calls this for an elastic index (deletes the index from oak repo)
                        // and then does a NOOP in postDeleteOp - effectively we are left with a dangling elastic remote index in this case.
                        filteredIndexPaths.add(repositoryIndexPath);
                    }
                }
            }
        }
        return filteredIndexPaths;
    }

    private List<IndexName> getIndexNameObjectList(Set<String> versionedIndexPaths) {
        List<IndexName> indexNameObjectList = new ArrayList<>();
        for (String indexNameString : versionedIndexPaths) {
            indexNameObjectList.add(IndexName.parse(indexNameString));
        }
        return indexNameObjectList;
    }

    private void purgeOldIndexVersion(NodeStore store, List<IndexVersionOperation> toDeleteIndexNameObjectList) throws
            CommitFailedException {
        for (IndexVersionOperation toDeleteIndexNameObject : toDeleteIndexNameObjectList) {
            NodeState root = store.getRoot();
            NodeBuilder rootBuilder = root.builder();
            String nodeName = toDeleteIndexNameObject.getIndexName().getNodeName();
            NodeBuilder nodeBuilder = PurgeOldVersionUtils.getNode(rootBuilder, nodeName);
            if (nodeBuilder.exists()) {
                String type = nodeBuilder.getProperty(TYPE_PROPERTY_NAME).getValue(Type.STRING);

                if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE_HIDDEN_AND_DISABLE) {
                    LOG.info("Disabling {}", nodeName);
                    nodeBuilder.setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED, Type.STRING);
                    // Set this property orig_type so that when the purge job marks this index for deletion in later runs -
                    // the proper post deletion hook can be called based on the original index type.
                    nodeBuilder.setProperty(ORIGINAL_TYPE_PROPERTY_NAME, type, Type.STRING);
                    EditorHook hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
                    store.merge(rootBuilder, hook, CommitInfo.EMPTY);
                    PurgeOldVersionUtils.recursiveDeleteHiddenChildNodes(store, nodeName);
                } else if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE) {
                    preserveDetailsFromIndexDefForPostOp(nodeBuilder);
                    LOG.info("Deleting {}", nodeName);
                    nodeBuilder.remove();
                    EditorHook hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
                    store.merge(rootBuilder, hook, CommitInfo.EMPTY);
                    postDeleteOp(toDeleteIndexNameObject.getIndexName().getNodeName());
                }
            } else {
                LOG.error("nodebuilder null for path " + nodeName);
            }
        }
    }

    /**
     *
     * @return IndexType served by the implementation like lucene or elasticsearch
     */
    protected abstract String getIndexType();

    /**
     *
     * @param idxPath - index path on which to perform post delete operations
     */
    protected abstract void postDeleteOp(String idxPath);

    /**
     *
     * @return true if the isIndexActiveCheck should be skipped
     * - for cases where index implementation does not have hidden oak mounts
     */
    protected abstract boolean returnIgnoreIsIndexActiveCheck();

    /**
     * To preserve any required details from index def builder to be used in post op
     */
    protected abstract void preserveDetailsFromIndexDefForPostOp(NodeBuilder builder);

    @Override
    public void close() throws IOException {
        closer.close();
    }

}
