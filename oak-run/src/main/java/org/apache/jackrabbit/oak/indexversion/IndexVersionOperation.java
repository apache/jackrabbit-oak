package org.apache.jackrabbit.oak.indexversion;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/*
    Main operation of this class is to mark IndexName's with operations
 */
public class IndexVersionOperation {

    /**
     * These operations are in ascending order and only one will be executed at last
     */
    enum Operation {
        NOOP, DELETE_HIDDEN_AND_DISABLE, DELETE
    }

    private static final Logger log = LoggerFactory.getLogger(IndexVersionOperation.class);
    private static String INDEX_DEFINITION_NODE = ":index-definition";
    private static String REINDEX_COMPLETION_TIMESTAMP = "reindexCompletionTimestamp";

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


    public static List<IndexVersionOperation> apply(NodeState indexDefParentNode,
                                                    List<IndexName> indexNameObjectList, int threshold) {
        int activeProductVersion = -1;
        //boolean isValidOperations = false;
        int activeCustomerVersion = -1;
        indexNameObjectList = getSortedVersionedIndexNameObjects(indexNameObjectList);
        List<IndexVersionOperation> toDeleteIndexNameObjectList = new LinkedList<>();
//        for (int i = indexNameObjectList.size() - 1; i >= 0; i--) {
        for (int i = 0; i < indexNameObjectList.size(); i++) {
            NodeState indexNode = indexDefParentNode
                    .getChildNode(indexNameObjectList.get(i).getNodeName());
            if (activeProductVersion == -1) {
                toDeleteIndexNameObjectList.add(new IndexVersionOperation(indexNameObjectList.get(i)));
                if (indexNode.hasChildNode(INDEX_DEFINITION_NODE)) {
                    if (indexNode.getChildNode(INDEX_DEFINITION_NODE)
                            .getProperty(REINDEX_COMPLETION_TIMESTAMP) != null) {
                        String reindexCompletionTime = indexDefParentNode
                                .getChildNode(indexNameObjectList.get(i).getNodeName())
                                .getChildNode(INDEX_DEFINITION_NODE)
                                .getProperty(REINDEX_COMPLETION_TIMESTAMP).getValue(Type.DATE);
                        long reindexCompletionTimeInMillis = PurgeOldVersionUtils.getMillisFromString(reindexCompletionTime);
                        long currentTimeInMillis = System.currentTimeMillis();
                        if (currentTimeInMillis - reindexCompletionTimeInMillis > threshold) {
                            activeProductVersion = indexNameObjectList.get(i).getProductVersion();
                            activeCustomerVersion = indexNameObjectList.get(i).getCustomerVersion();
                            //if (activeCustomerVersion == 0) {
                            //isValidOperations = true;
                            //}
                        }
                    } else {
                        log.warn(REINDEX_COMPLETION_TIMESTAMP
                                + " property is not set for index " + indexNameObjectList.get(i).getNodeName());
                    }
                } else {

                }
            } else {
                if (indexNameObjectList.get(i).getProductVersion() == activeProductVersion
                        && indexNameObjectList.get(i).getCustomerVersion() == 0) {
                    IndexVersionOperation indexVersionOperation = new IndexVersionOperation(indexNameObjectList.get(i));
                    indexVersionOperation.setOperation(Operation.DELETE_HIDDEN_AND_DISABLE);
                    toDeleteIndexNameObjectList.add(indexVersionOperation);
                    //isValidOperations = true;
                } else {
                    IndexVersionOperation indexVersionOperation = new IndexVersionOperation(indexNameObjectList.get(i));
                    indexVersionOperation.setOperation(Operation.DELETE);
                    toDeleteIndexNameObjectList.add(indexVersionOperation);
                }
            }
        }
        if (!isValidIndexVersionOperationList(toDeleteIndexNameObjectList)) {
            //log.info("IndexVersionOperation List is not valid for index {}");
            toDeleteIndexNameObjectList = Collections.EMPTY_LIST;
        }
        return toDeleteIndexNameObjectList;
    }

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
//        for (IndexVersionOperation indexVersionOperation : indexVersionOperations) {
        for (int i = 0; i < indexVersionOperations.size(); i++) {
            if (indexVersionOperations.get(i).getOperation() == Operation.NOOP){
                lastNoopOperationIndexVersion = indexVersionOperations.get(i);
            }
            if (indexVersionOperations.get(i).getOperation() == Operation.DELETE_HIDDEN_AND_DISABLE){
                indexWithDeleteHiddenOp = indexVersionOperations.get(i);
            }
        }

        if (lastNoopOperationIndexVersion.getIndexName().getCustomerVersion() == 0){
            isValid = true;
        }
        else if (lastNoopOperationIndexVersion.getIndexName().getCustomerVersion() != 0) {
            if (indexWithDeleteHiddenOp != null) {
            isValid = true;
            }
        }

        if (!isValid) {
            log.info("IndexVersionOperation List is not valid for index {}", lastNoopOperationIndexVersion.getIndexName().getNodeName());
        }
        return isValid;
    }

    @Override
    public String toString() {
        return this.getIndexName() +" operation:"+this.getOperation();
    }
}


