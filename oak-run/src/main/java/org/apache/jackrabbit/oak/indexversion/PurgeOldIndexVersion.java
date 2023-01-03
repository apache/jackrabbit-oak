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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class PurgeOldIndexVersion {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeOldIndexVersion.class);

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

    public List<IndexVersionOperation> getPurgeIndexes(NodeStore nodeStore, long purgeThresholdMillis, List<String> indexPaths, boolean shouldPurgeBaseIndex ) throws IOException, CommitFailedException {
        List<IndexVersionOperation> purgeIndexList = new ArrayList<>();
        LOG.info("Getting indexes to purge over index paths '{}'", indexPaths);
        List<String> sanitisedIndexPaths = sanitiseUserIndexPaths(indexPaths);
        Set<String> indexPathSet = filterIndexPaths(getRepositoryIndexPaths(nodeStore), sanitisedIndexPaths);
        Map<String, Set<String>> segregateIndexes = segregateIndexes(indexPathSet);
        for (Map.Entry<String, Set<String>> entry : segregateIndexes.entrySet()) {
            String baseIndexPath = entry.getKey();
            String parentPath = PathUtils.getParentPath(baseIndexPath);
            List<IndexName> indexNameObjectList = getIndexNameObjectList(entry.getValue());
            LOG.info("Validate purge index over base of '{}', which includes: '{}'", baseIndexPath, indexNameObjectList);
            List<IndexVersionOperation> toDeleteIndexNameObjectList = IndexVersionOperation.generateIndexVersionOperationList(
                    nodeStore.getRoot(), parentPath, indexNameObjectList, purgeThresholdMillis, shouldPurgeBaseIndex);
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
    private Set<String> filterIndexPaths(Iterable<String> repositoryIndexPaths, List<String> commandlineIndexPaths) {
        Set<String> filteredIndexPaths = new HashSet<>();
        for (String commandlineIndexPath : commandlineIndexPaths) {
            for (String repositoryIndexPath : repositoryIndexPaths) {
                if (PurgeOldVersionUtils.isIndexChildNode(commandlineIndexPath, repositoryIndexPath)
                        || PurgeOldVersionUtils.isBaseIndexEqual(commandlineIndexPath, repositoryIndexPath)) {
                    filteredIndexPaths.add(repositoryIndexPath);
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
                if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE_HIDDEN_AND_DISABLE) {
                    LOG.info("Disabling {}", nodeName);
                    nodeBuilder.setProperty("type", "disabled", Type.STRING);
                    EditorHook hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
                    store.merge(rootBuilder, hook, CommitInfo.EMPTY);
                    PurgeOldVersionUtils.recursiveDeleteHiddenChildNodes(store, nodeName);
                } else if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE) {
                    LOG.info("Deleting {}", nodeName);
                    nodeBuilder.remove();
                    EditorHook hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
                    store.merge(rootBuilder, hook, CommitInfo.EMPTY);
                }
            } else {
                LOG.error("nodebuilder null for path " + nodeName);
            }
        }
    }
}
