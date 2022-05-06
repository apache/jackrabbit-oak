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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
    Main operation of this class is to mark IndexName's with operations
 */
public class IndexVersionOperation {
    private static final Logger LOG = LoggerFactory.getLogger(IndexVersionOperation.class);

    private IndexName indexName;
    private Operation operation;

    public IndexVersionOperation(IndexName indexName) {
        this.indexName = indexName;
        this.operation = Operation.NOOP;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return this.operation;
    }

    public IndexName getIndexName() {
        return this.indexName;
    }

    @Override
    public String toString() {
        return this.getIndexName() + " operation:" + this.getOperation();
    }

    /**
     * Generate list of index version operation over a list of indexes have same index base.
     *
     * @param rootNode             NodeState of root
     * @param parentPath           parent path of baseIndex
     * @param indexNameObjectList  This is a list of IndexName Objects with same baseIndexName on which operations will be applied.
     * @param purgeThresholdMillis after which a fully functional index is eligible for purge operations
     *
     * @return This method returns an IndexVersionOperation list i.e indexNameObjectList marked with operations
     */
    public static List<IndexVersionOperation> generateIndexVersionOperationList(NodeState rootNode, String parentPath,
            List<IndexName> indexNameObjectList, long purgeThresholdMillis) {
        NodeState indexDefParentNode = NodeStateUtils.getNode(rootNode, parentPath);
        List<IndexName> reverseSortedIndexNameList = getReverseSortedIndexNameList(indexNameObjectList);
        List<IndexVersionOperation> indexVersionOperationList = new LinkedList<>();

        List<IndexName> disableIndexNameObjectList = removeDisabledCustomIndexesFromList(indexDefParentNode, reverseSortedIndexNameList);
        // for disabled indexes, we check if they exist in read-only repo, it not anymore, do full deletion then, otherwise, no action needed
        for (IndexName indexNameObject : disableIndexNameObjectList) {
            String indexName = indexNameObject.getNodeName();
            NodeState indexNode = indexDefParentNode.getChildNode(PathUtils.getName(indexName));
            IndexVersionOperation indexVersionOperation = new IndexVersionOperation(indexNameObject);
            if (!isHiddenOakMountExists(indexNode)) {
                indexVersionOperation.setOperation(Operation.DELETE);
            }
            indexVersionOperationList.add(indexVersionOperation);
        }

        if (!reverseSortedIndexNameList.isEmpty()) {
            IndexName activeIndexNameObject = getActiveIndex(reverseSortedIndexNameList, parentPath, rootNode);
            if (activeIndexNameObject == null) {
                LOG.warn("Cannot find any active index from the list: {}", reverseSortedIndexNameList);
            } else {
                NodeState activeIndexNode = indexDefParentNode.getChildNode(PathUtils.getName(activeIndexNameObject.getNodeName()));
                boolean isActiveIndexLongEnough = isIndexPurgeReady(activeIndexNameObject, activeIndexNode, purgeThresholdMillis);
                int activeProductVersion = activeIndexNameObject.getProductVersion();
                indexVersionOperationList.add(new IndexVersionOperation(activeIndexNameObject));

                // the reverseSortedIndexNameList will now contain the remaining indexes,
                // the active index and disabled index was removed from that list already
                for (IndexName indexNameObject : reverseSortedIndexNameList) {
                    String indexName = indexNameObject.getNodeName();
                    NodeState indexNode = indexDefParentNode.getChildNode(PathUtils.getName(indexName));
                    IndexVersionOperation indexVersionOperation = new IndexVersionOperation(indexNameObject);
                    // if active index not long enough, NOOP for all indexes
                    if (isActiveIndexLongEnough) {
                        if (indexNameObject.getProductVersion() == activeProductVersion && indexNameObject.getCustomerVersion() == 0) {
                            indexVersionOperation.setOperation(Operation.DELETE_HIDDEN_AND_DISABLE);
                        } else if (indexNameObject.getProductVersion() <= activeProductVersion ) {
                            // the check hidden oak mount logic only works when passing through the proper composite store
                            if (isHiddenOakMountExists(indexNode)) {
                                LOG.info("Found hidden oak mount node for: '{}', disable it but no index definition deletion", indexName);
                                indexVersionOperation.setOperation(Operation.DELETE_HIDDEN_AND_DISABLE);
                            } else {
                                indexVersionOperation.setOperation(Operation.DELETE);
                            }
                        } else {
                            // if the index product version is larger than active index, leave it as is
                            // for instance: if there is active damAssetLucene-7, the inactive damAssetLucene-8 will be leave there as is
                            LOG.info("The index '{}' leave as is since the version is larger than current active index", indexName);
                            indexVersionOperation.setOperation(Operation.NOOP);
                        }
                    }
                    LOG.info("The operation for index '{}' will be: '{}'", indexName, indexVersionOperation.getOperation());
                    indexVersionOperationList.add(indexVersionOperation);
                }
            }
        }
        if (indexVersionOperationList.isEmpty() || !isValidIndexVersionOperationList(indexVersionOperationList)) {
            LOG.info("Not valid version operation list: '{}', skip all", indexNameObjectList);
            indexVersionOperationList = Collections.emptyList();
        }
        return indexVersionOperationList;
    }

    // iterate all indexes from high version to lower version to find the active index, then remove it from the reverseSortedIndexNameList
    private static IndexName getActiveIndex(List<IndexName> reverseSortedIndexNameList, String parentPath, NodeState rootNode) {
        for (int i = 0; i < reverseSortedIndexNameList.size(); i++) {
            IndexName indexNameObject = reverseSortedIndexNameList.get(i);
            String indexName = indexNameObject.getNodeName();
            String indexPath = PathUtils.concat(parentPath, PathUtils.getName(indexName));
            if (IndexName.isIndexActive(indexPath, rootNode)) {
                LOG.info("Found active index '{}'", indexPath);
                reverseSortedIndexNameList.remove(i);
                return indexNameObject;
            } else {
                LOG.info("The index '{}' isn't active", indexPath);
            }
        }
        return null;
    }

    // do index purge ready based on the active index's last reindexing time is longer enough, we do this for prevent rollback
    private static boolean isIndexPurgeReady(IndexName activeIndexName, NodeState activeIndexNode, long purgeThresholdMillis) {
        // the 1st index must be active
        String indexName = activeIndexName.getNodeName();
        if (activeIndexNode.hasChildNode(IndexDefinition.STATUS_NODE)) {
            if (activeIndexNode.getChildNode(IndexDefinition.STATUS_NODE)
                    .getProperty(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP) != null) {
                String reindexCompletionTime = activeIndexNode.getChildNode(IndexDefinition.STATUS_NODE)
                        .getProperty(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP)
                        .getValue(Type.DATE);
                long reindexCompletionTimeInMillis = PurgeOldVersionUtils.getMillisFromString(reindexCompletionTime);
                long currentTimeInMillis = System.currentTimeMillis();
                if (currentTimeInMillis - reindexCompletionTimeInMillis > purgeThresholdMillis) {
                    LOG.info("Found active index {} is longer enough", indexName);
                    return true;
                } else {
                    LOG.info("The last index time '{}' isn't longer enough for: {}", reindexCompletionTime, indexName);
                }
            } else {
                LOG.warn("{} property is not set for index {}", IndexDefinition.REINDEX_COMPLETION_TIMESTAMP, indexName);
            }
        }
        LOG.info("The active index '{}' indexing time isn't long enough", indexName);
        return false;
    }

    private static boolean isHiddenOakMountExists(NodeState indexNode) {
        for (String nodeName : indexNode.getChildNodeNames()) {
            if (nodeName.startsWith(IndexDefinition.HIDDEN_OAK_MOUNT_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    /*
        returns IndexNameObjects in descending order of version i.e from newest version to oldest
    */
    private static List<IndexName> getReverseSortedIndexNameList(List<IndexName> indexNameObjectList) {
        List<IndexName> reverseSortedIndexNameObjectList = new ArrayList<>(indexNameObjectList);
        Collections.sort(reverseSortedIndexNameObjectList, Collections.reverseOrder());
        return reverseSortedIndexNameObjectList;
    }

    /**
     * @param indexVersionOperations
     * @return true if the IndexVersionOperation list passes following criteria.
     * For merging indexes we need baseIndex and latest custom index.
     * So we first validate that if there are custom indexes than OOTB index with same product must be marked with DELETE_HIDDEN_AND_DISABLE
     */
    private static boolean isValidIndexVersionOperationList(List<IndexVersionOperation> indexVersionOperations) {
        boolean isValid = false;
        IndexVersionOperation lastNoopOperationIndexVersion = null;
        IndexVersionOperation indexWithDeleteHiddenOp = null;
        for (IndexVersionOperation indexVersionOperation : indexVersionOperations) {
            if (indexVersionOperation.getOperation() == Operation.NOOP) {
                if (lastNoopOperationIndexVersion == null) {
                    lastNoopOperationIndexVersion = indexVersionOperation;
                }
            }
            if (indexVersionOperation.getOperation() == Operation.DELETE_HIDDEN_AND_DISABLE) {
                if (indexWithDeleteHiddenOp == null) {
                    indexWithDeleteHiddenOp = indexVersionOperation;
                }
            }
        }
        if (lastNoopOperationIndexVersion.getIndexName().getCustomerVersion() == 0) {
            isValid = true;
        } else if (lastNoopOperationIndexVersion.getIndexName().getCustomerVersion() != 0) {
            if (indexWithDeleteHiddenOp != null
                    && lastNoopOperationIndexVersion.getIndexName().getProductVersion() == indexWithDeleteHiddenOp.getIndexName().getProductVersion()) {
                isValid = true;
            }
        }
        if (!isValid) {
            LOG.info("IndexVersionOperation List is not valid for index {}", lastNoopOperationIndexVersion.getIndexName().getNodeName());
        }
        return isValid;
    }

    private static List<IndexName> removeDisabledCustomIndexesFromList(NodeState indexDefParentNode,
                                                            List<IndexName> indexNameObjectList) {
        List<IndexName> disableIndexNameObjectList = new ArrayList<>();
        for (int i = 0; i < indexNameObjectList.size(); i++) {
            NodeState indexNode = indexDefParentNode
                    .getChildNode(PathUtils.getName(indexNameObjectList.get(i).getNodeName()));
            if (indexNode.getProperty("type") != null && "disabled".equals(indexNode.getProperty("type").getValue(Type.STRING))) {
                disableIndexNameObjectList.add(indexNameObjectList.get(i));
                indexNameObjectList.remove(i);
            }
        }
        return disableIndexNameObjectList;
    }

    /*
        NOOP means : No operation to be performed index Node
        DELETE_HIDDEN_AND_DISABLE: This operation means that we should disable this indexNode and delete all hidden nodes under it
        DELETE: Delete this index altogether
     */
    enum Operation {
        NOOP,
        DELETE_HIDDEN_AND_DISABLE,
        DELETE
    }
}


