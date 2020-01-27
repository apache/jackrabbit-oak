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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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
     * @param indexDefParentNode   NodeState of parent of baseIndex
     * @param indexNameObjectList  This is a list of IndexName Objects with same baseIndexName on which operations will be applied.
     * @param purgeThresholdMillis after which a fully functional index is eligible for purge operations
     * @return This method returns an IndexVersionOperation list i.e indexNameObjectList marked with operations
     */
    public static List<IndexVersionOperation> generateIndexVersionOperationList(NodeState indexDefParentNode,
                                                                                List<IndexName> indexNameObjectList, long purgeThresholdMillis) {
        int activeProductVersion = -1;
        List<IndexName> reverseSortedIndexNameList = getReverseSortedIndexNameList(indexNameObjectList);
        removeDisabledCustomIndexesFromList(indexDefParentNode, reverseSortedIndexNameList);
        List<IndexVersionOperation> indexVersionOperationList = new LinkedList<>();
        for (IndexName indexNameObject : reverseSortedIndexNameList) {
            NodeState indexNode = indexDefParentNode
                    .getChildNode(PathUtils.getName(indexNameObject.getNodeName()));
            /*
            All Indexes are by default NOOP until we get index which is active for more than threshold limit
             */
            if (activeProductVersion == -1) {
                indexVersionOperationList.add(new IndexVersionOperation(indexNameObject));
                if (indexNode.hasChildNode(IndexDefinition.STATUS_NODE)) {
                    if (indexNode.getChildNode(IndexDefinition.STATUS_NODE)
                            .getProperty(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP) != null) {
                        String reindexCompletionTime = indexDefParentNode
                                .getChildNode(PathUtils.getName(indexNameObject.getNodeName()))
                                .getChildNode(IndexDefinition.STATUS_NODE)
                                .getProperty(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP).getValue(Type.DATE);
                        long reindexCompletionTimeInMillis = PurgeOldVersionUtils.getMillisFromString(reindexCompletionTime);
                        long currentTimeInMillis = System.currentTimeMillis();
                        if (currentTimeInMillis - reindexCompletionTimeInMillis > purgeThresholdMillis) {
                            activeProductVersion = indexNameObject.getProductVersion();
                        }
                    } else {
                        LOG.warn(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP
                                + " property is not set for index " + indexNameObject.getNodeName());
                    }
                }
            } else {
            /*
            Once we found active index, we mark all its custom version with DELETE
            and OOTB same product version index with DELETE_HIDDEN_AND_DISABLE
             */
                if (indexNameObject.getProductVersion() == activeProductVersion
                        && indexNameObject.getCustomerVersion() == 0) {
                    IndexVersionOperation indexVersionOperation = new IndexVersionOperation(indexNameObject);
                    indexVersionOperation.setOperation(Operation.DELETE_HIDDEN_AND_DISABLE);
                    indexVersionOperationList.add(indexVersionOperation);
                } else {
                    IndexVersionOperation indexVersionOperation = new IndexVersionOperation(indexNameObject);
                    indexVersionOperation.setOperation(Operation.DELETE);
                    indexVersionOperationList.add(indexVersionOperation);
                }
            }
        }
        if (!isValidIndexVersionOperationList(indexVersionOperationList)) {
            indexVersionOperationList = Collections.emptyList();
        }
        return indexVersionOperationList;
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
                lastNoopOperationIndexVersion = indexVersionOperation;
            }
            if (indexVersionOperation.getOperation() == Operation.DELETE_HIDDEN_AND_DISABLE) {
                indexWithDeleteHiddenOp = indexVersionOperation;
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

    private static void removeDisabledCustomIndexesFromList(NodeState indexDefParentNode,
                                                            List<IndexName> indexNameObjectList) {
        for (int i = 0; i < indexNameObjectList.size(); i++) {
            NodeState indexNode = indexDefParentNode
                    .getChildNode(PathUtils.getName(indexNameObjectList.get(i).getNodeName()));
            if (indexNode.getProperty("type") != null && "disabled".equals(indexNode.getProperty("type").getValue(Type.STRING))) {
                indexNameObjectList.remove(i);
            }
        }
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


