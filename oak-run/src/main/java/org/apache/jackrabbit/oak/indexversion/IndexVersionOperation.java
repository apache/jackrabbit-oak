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
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/*
    Main operation of this class is to mark IndexName's with operations
 */
public class IndexVersionOperation {

    enum Operation {
        NOOP, DELETE_HIDDEN_AND_DISABLE, DELETE
    }

    private static final Logger log = LoggerFactory.getLogger(IndexVersionOperation.class);

    private IndexName indexName;

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return this.operation;
    }

    public IndexName getIndexName() {
        return this.indexName;
    }

    private Operation operation;

    public IndexVersionOperation(IndexName indexName) {
        this.indexName = indexName;
        this.operation = Operation.NOOP;
    }


    public static List<IndexVersionOperation> applyOperation(NodeState indexDefParentNode,
                                                             List<IndexName> indexNameObjectList, long threshold) {
        int activeProductVersion = -1;
        indexNameObjectList = getSortedVersionedIndexNameObjects(indexNameObjectList);
        removeDisabledCustomIndexes(indexDefParentNode, indexNameObjectList);
        List<IndexVersionOperation> indexVersionOperationList = new LinkedList<>();
        for (int i = 0; i < indexNameObjectList.size(); i++) {
            NodeState indexNode = indexDefParentNode
                    .getChildNode(indexNameObjectList.get(i).getNodeName());
            if (activeProductVersion == -1) {
                indexVersionOperationList.add(new IndexVersionOperation(indexNameObjectList.get(i)));
                if (indexNode.hasChildNode(IndexDefinition.STATUS_NODE)) {
                    if (indexNode.getChildNode(IndexDefinition.STATUS_NODE)
                            .getProperty(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP) != null) {
                        String reindexCompletionTime = indexDefParentNode
                                .getChildNode(indexNameObjectList.get(i).getNodeName())
                                .getChildNode(IndexDefinition.STATUS_NODE)
                                .getProperty(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP).getValue(Type.DATE);
                        long reindexCompletionTimeInMillis = PurgeOldVersionUtils.getMillisFromString(reindexCompletionTime);
                        long currentTimeInMillis = System.currentTimeMillis();
                        if (currentTimeInMillis - reindexCompletionTimeInMillis > threshold) {
                            activeProductVersion = indexNameObjectList.get(i).getProductVersion();
                        }
                    } else {
                        log.warn(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP
                                + " property is not set for index " + indexNameObjectList.get(i).getNodeName());
                    }
                }
            } else {
                if (indexNameObjectList.get(i).getProductVersion() == activeProductVersion
                        && indexNameObjectList.get(i).getCustomerVersion() == 0) {
                    IndexVersionOperation indexVersionOperation = new IndexVersionOperation(indexNameObjectList.get(i));
                    indexVersionOperation.setOperation(Operation.DELETE_HIDDEN_AND_DISABLE);
                    indexVersionOperationList.add(indexVersionOperation);
                } else {
                    IndexVersionOperation indexVersionOperation = new IndexVersionOperation(indexNameObjectList.get(i));
                    indexVersionOperation.setOperation(Operation.DELETE);
                    indexVersionOperationList.add(indexVersionOperation);
                }
            }
        }
        if (!isValidIndexVersionOperationList(indexVersionOperationList)) {
            indexVersionOperationList = Collections.EMPTY_LIST;
        }
        return indexVersionOperationList;
    }

    /*
        returns IndexNameObjects in descending order of version i.e from latest to oldest
    */
    private static List<IndexName> getSortedVersionedIndexNameObjects(List<IndexName> indexNameObjectList) {
        indexNameObjectList.sort(new Comparator<IndexName>() {
            @Override
            public int compare(IndexName indexNameObj1, IndexName indexNameObj2) {
                return indexNameObj2.compareTo(indexNameObj1);
            }
        });
        return indexNameObjectList;
    }

    private static boolean isValidIndexVersionOperationList(List<IndexVersionOperation> indexVersionOperations) {
        boolean isValid = false;
        IndexVersionOperation lastNoopOperationIndexVersion = null;
        IndexVersionOperation indexWithDeleteHiddenOp = null;
        for (int i = 0; i < indexVersionOperations.size(); i++) {
            if (indexVersionOperations.get(i).getOperation() == Operation.NOOP) {
                lastNoopOperationIndexVersion = indexVersionOperations.get(i);
            }
            if (indexVersionOperations.get(i).getOperation() == Operation.DELETE_HIDDEN_AND_DISABLE) {
                indexWithDeleteHiddenOp = indexVersionOperations.get(i);
            }
        }
        if (lastNoopOperationIndexVersion.getIndexName().getCustomerVersion() == 0) {
            isValid = true;
        } else if (lastNoopOperationIndexVersion.getIndexName().getCustomerVersion() != 0) {
            if (indexWithDeleteHiddenOp != null) {
                isValid = true;
            }
        }
        if (!isValid) {
            log.info("IndexVersionOperation List is not valid for index {}", lastNoopOperationIndexVersion.getIndexName().getNodeName());
        }
        return isValid;
    }

    private static void removeDisabledCustomIndexes(NodeState indexDefParentNode,
                                                    List<IndexName> indexNameObjectList) {
        for (int i = 0; i < indexNameObjectList.size(); i++) {
            NodeState indexNode = indexDefParentNode
                    .getChildNode(indexNameObjectList.get(i).getNodeName());
            if (indexNode.getProperty("type") != null && indexNode.getProperty("type").getValue(Type.STRING).equals("disabled")) {
                indexNameObjectList.remove(i);
            }
        }
    }

    @Override
    public String toString() {
        return this.getIndexName() + " operation:" + this.getOperation();
    }
}


