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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PurgeOldIndexVersion {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeOldIndexVersion.class);

    public void execute(Options opts, long purgeThresholdMillis, List<String> indexPaths) throws Exception {
        boolean isReadWriteRepository = opts.getCommonOpts().isReadWrite();
        if (!isReadWriteRepository) {
            LOG.info("Repository connected in read-only mode. Use '--read-write' for write operations");
        }
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            NodeStore nodeStore = fixture.getStore();
            List<String> sanitisedIndexPaths = sanitiseUserIndexPaths(indexPaths);
            Set<String> indexPathSet = filterIndexPaths(getRepositoryIndexPaths(nodeStore), sanitisedIndexPaths);
            Map<String, Set<String>> segregateIndexes = segregateIndexes(indexPathSet);
            for (Map.Entry<String, Set<String>> entry : segregateIndexes.entrySet()) {
                String baseIndexPath = entry.getKey();
                String parentPath = PathUtils.getParentPath(entry.getKey());
                List<IndexName> indexNameObjectList = getIndexNameObjectList(entry.getValue());
                NodeState indexDefParentNode = NodeStateUtils.getNode(nodeStore.getRoot(),
                        parentPath);
                List<IndexVersionOperation> toDeleteIndexNameObjectList =
                        IndexVersionOperation.generateIndexVersionOperationList(indexDefParentNode, indexNameObjectList, purgeThresholdMillis);
                if (isReadWriteRepository && !toDeleteIndexNameObjectList.isEmpty()) {
                    purgeOldIndexVersion(nodeStore, toDeleteIndexNameObjectList);
                } else {
                    LOG.info("Repository is opened in read-only mode: IndexOperations" +
                            " for index at path {} are : {}", baseIndexPath, toDeleteIndexNameObjectList);
                }
            }
        }
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

    private void purgeOldIndexVersion(NodeStore store,
                                      List<IndexVersionOperation> toDeleteIndexNameObjectList) throws CommitFailedException {
        for (IndexVersionOperation toDeleteIndexNameObject : toDeleteIndexNameObjectList) {
            NodeState root = store.getRoot();
            NodeBuilder rootBuilder = root.builder();
            NodeBuilder nodeBuilder = PurgeOldVersionUtils.getNode(rootBuilder, toDeleteIndexNameObject.getIndexName().getNodeName());
            if (nodeBuilder.exists()) {
                if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE_HIDDEN_AND_DISABLE) {
                    nodeBuilder.setProperty("type", "disabled", Type.STRING);
                    PurgeOldVersionUtils.recursiveDeleteHiddenChildNodes(store, toDeleteIndexNameObject.getIndexName().getNodeName());
                } else if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE) {
                    nodeBuilder.remove();
                }
                EditorHook hook = new EditorHook(
                        new IndexUpdateProvider(new PropertyIndexEditorProvider()));
                store.merge(rootBuilder, hook, CommitInfo.EMPTY);
            } else {
                LOG.error("nodebuilder null for path " + toDeleteIndexNameObject.getIndexName().getNodeName());
            }
        }
    }
}
