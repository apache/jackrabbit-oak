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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.oak.plugins.document.FullGcNodeBin;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.Document.ID;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_REMOVED_TOTAL_BSON_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_ID;

/**
 * This class is a wrapper around a MongoFullGcNodeBin
 * that sums the bson size of the documents that are removed or updated and then update the value in the SETTINGS collection
 */
class MongoFullGcNodeBinSumBsonSize implements FullGcNodeBin {
    private static final Logger LOG = LoggerFactory.getLogger(MongoFullGcNodeBinSumBsonSize.class);

    private final MongoFullGcNodeBin delegate;
    private final MongoDocumentStore store;

    public MongoFullGcNodeBinSumBsonSize(MongoFullGcNodeBin delegate) {
        this.delegate = delegate;
        this.store = delegate.getMongoDocumentStore();
    }

    @Override
    public void setEnabled(boolean value) {
        delegate.setEnabled(value);
    }

    /**
     * Remove the documents from the collection and sum the the bson size of the removed properties     
     * @param updateOpList the list of the documents to be removed
     * @return the list of the documents removed
     */ 
    @Override
    public List<NodeDocument> findAndUpdate(List<UpdateOp> updateOpList) {
        List<String> ids = updateOpList.stream().map(UpdateOp::getId).collect(Collectors.toList());
        //get the total bson size before the update
        long initialBsonSize = getBsonSize(ids);
        LOG.debug("bson size before update: {}", initialBsonSize);
        //remove garbage properties
        List<NodeDocument> updated = delegate.findAndUpdate(updateOpList);
        if (!updated.isEmpty()) {
            //calculate the diff of the bson size after update
            long afterUpdateBsonSize = getBsonSize(ids);
            LOG.debug("bson size after update: {}", afterUpdateBsonSize);
            if (initialBsonSize > 0 && afterUpdateBsonSize > 0) {
                //sum up the removed bson size
                addBsonSize(initialBsonSize - afterUpdateBsonSize);
            }
        }
        return updated;
    }
    /**
     * Remove the documents from the collection and sum the their bson size      
     * @param orphanOrDeletedRemovalMap the map of the documents to be removed
     * @return the number of documents removed
     */
    @Override
    public int remove(Map<String, Long> orphanOrDeletedRemovalMap) {
        //get the total bson size before the update
        long bsonSize = getBsonSize(new ArrayList<>(orphanOrDeletedRemovalMap.keySet()));
        LOG.debug("bson size before remove: {}", bsonSize);
        //remove garbage documents
        int removed = delegate.remove(orphanOrDeletedRemovalMap);
        //sum up the removed bson size
        if (removed > 0 && bsonSize > 0) {
            addBsonSize(bsonSize);
        }
        return removed;
    }

    private void addBsonSize(long bsonSize) {
        if (bsonSize <= 0) {
            LOG.warn("bson size {} is not positive", bsonSize);
            return;
        }
        //sum the bson size with the value from fullGcBsonSize document in the SETTINGS collection
        Bson query = eq(ID, SETTINGS_COLLECTION_ID);
        Bson update = Updates.inc(SETTINGS_COLLECTION_FULL_GC_REMOVED_TOTAL_BSON_SIZE, bsonSize);
        //increment the value in SETTINGS collection with the new bson size
        store.getDBCollection(SETTINGS).updateOne(query, update);
        LOG.info("Incremented bson size with {}", bsonSize);
    }

    /**
     * Calculate the total bson size of documents in the list
     * @param ids the list of ids to be iterated
     * @return the total size of the bson
     */
    private long getBsonSize(List<String> ids) {
        long start = System.currentTimeMillis();
        try {
            //get the bson size of the documents in the list
            Bson match = in("_id", ids);
            BasicDBObject first = store.getDBCollection(NODES).aggregate(List.of(
                new BasicDBObject("$match", match),
                new BasicDBObject("$group", new BasicDBObject("_id", null)
                    .append("totalSize", new BasicDBObject("$sum", new BasicDBObject("$bsonSize", "$$ROOT"))))
            )).first();
            return first != null ? ((Number) first.get("totalSize")).longValue() : -1;
        } finally {
            LOG.info("getBsonSize for {} documents took {} ms", ids.size(), System.currentTimeMillis() - start);
        }
    }
}
