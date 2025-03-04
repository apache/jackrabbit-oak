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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.BasicDBObject;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.FullGcNodeBin;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  This class is as a wrapper around DocumentStore that expose two methods used to clean garbage from NODES collection
 *  public int remove(Map<String, Long> orphanOrDeletedRemovalMap)
 *  public List<NodeDocument> findAndUpdate(List<UpdateOp> updateOpList)
 *  When enabled
 *  Each method saves the document ID or empty properties names (that will be deleted) to a separate _bin collection as a BinDocument then delegates deletion to DocumentStore
 *
 *  When disabled (default)
 *  Each method delegates directly to DocumentStore
 */
public class MongoFullGcNodeBin implements FullGcNodeBin {
    public static final String GC_COLLECTED_AT = "_gcCollectedAt";
    private static final Logger LOG = LoggerFactory.getLogger(MongoFullGcNodeBin.class);

    private final MongoDocumentStore mongoDocumentStore;
    private boolean enabled;

    public MongoFullGcNodeBin(MongoDocumentStore ds) {
        this(ds, false);
    }

    public MongoFullGcNodeBin(MongoDocumentStore store, boolean fullGcBinEnabled) {
        mongoDocumentStore = store;
        enabled = fullGcBinEnabled;
    }

    /**
     * Remove orphaned or deleted documents from the NODES collection
     * If bin is enabled, the document IDs are saved to the BIN collection with ID prefixed with '/bin/'
     * If document ID cannot be saved then the removal of the document fails
     * If the bin is disabled, the document IDs are directly removed from the NODES collection
     *
     * @param orphanOrDeletedRemovalMap the keys of the documents to remove with the corresponding timestamps
     * @return the number of documents removed
     * @see DocumentStore#remove(Collection, Map)
     */
    @Override
    public int remove(Map<String, Long> orphanOrDeletedRemovalMap) {
        if (orphanOrDeletedRemovalMap.isEmpty() || !addToBin(orphanOrDeletedRemovalMap)) {
            return 0;
        }

        // use remove() with the modified check to rule
        // out any further race-condition where this removal
        // races with a un-orphan/re-creation as a result of which
        // the node should now not be removed. The modified check
        // ensures a node would then not be removed
        // (and as a result the removedSize != map.size())
        return mongoDocumentStore.remove(Collection.NODES, orphanOrDeletedRemovalMap);
    }


    /**
     * Performs a conditional update
     * If the bin is enabled, the removed properties are saved to the BIN collection with ID prefixed with '/bin/' and empty value
     * If the document ID and properties  cannot be saved then the removal of the property fails
     * If bin is disabled, the removed properties are directly removed from the NODES collection
     *
     * @param updateOpList the update operation List
     * @return the list containing old documents
     * @see DocumentStore#findAndUpdate(Collection, List)
     */
    @Override
    public List<NodeDocument> findAndUpdate(List<UpdateOp> updateOpList) {
        LOG.info("Updating {} documents", updateOpList.size());
        if (updateOpList.isEmpty() || !addToBin(updateOpList)) {
            return Collections.emptyList();
        }
        return mongoDocumentStore.findAndUpdate(Collection.NODES, updateOpList);
    }

    private boolean addToBin(Map<String, Long> orphanOrDeletedRemovalMap) {
        if (!enabled) {
            LOG.info("Bin is disabled, skipping adding delete candidate documents to bin");
            return true;
        }
        LOG.info("Adding {} delete candidate documents to bin", orphanOrDeletedRemovalMap.size());
        List<BasicDBObject> docs = orphanOrDeletedRemovalMap.keySet().stream()
            .map(e -> new UpdateOp(e, true))
            .map(this::toBasicDBObject)
            .collect(Collectors.toList());
        try {
            return persist(docs);
        } catch (Exception e) {
            LOG.error("Error while adding delete candidate documents to bin: {}", docs, e);
        }
        return false;
    }

    private boolean addToBin(List<UpdateOp> updateOpList) {
        if (!enabled) {
            LOG.info("Bin is disabled, skipping adding removed properties to bin");
            return true;
        }
        LOG.info("Adding {} removed properties to bin", updateOpList.size());
        List<BasicDBObject> binOpList = updateOpList.stream().map(this::toBasicDBObject).collect(Collectors.toList());
        try {
            return persist(binOpList);
        } catch (Exception e) {
            LOG.error("Error while adding removed properties to bin: {}", binOpList, e);
        }
        return false;
    }

    private boolean persist(List<BasicDBObject> inserts) {
        mongoDocumentStore.getBinCollection().insertMany(inserts);
        return true;
    }

    private BasicDBObject toBasicDBObject(UpdateOp op) {
        BasicDBObject doc = new BasicDBObject();
        doc.put(Document.ID, "/bin/" + op.getId() + "-" + Instant.now().toEpochMilli());
        //copy removed properties to the new document
        op.getChanges().forEach((k, v) -> {
            if (v.type == UpdateOp.Operation.Type.REMOVE) {
                doc.put(k.getName(), "");
            }
        });
        //this property is used to track the time when the document was added to the bin
        //it can be used as a TTL index property to automatically remove the document after a certain time
        //see https://www.mongodb.com/docs/manual/core/index-ttl/#std-label-index-feature-ttl
        doc.put(MongoFullGcNodeBin.GC_COLLECTED_AT, new Date());
        return doc;
    }

    @Override
    public void setEnabled(boolean value) {
        this.enabled = value;
        LOG.info("Full GC Bin changed to {}", enabled ? "enabled" : "disabled");
    }

    public boolean isEnabled() {
        return enabled;
    }
}
