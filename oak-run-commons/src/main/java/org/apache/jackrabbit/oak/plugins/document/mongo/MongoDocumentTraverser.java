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

package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.Objects;
import java.util.function.Predicate;

import com.google.common.collect.FluentIterable;
import com.mongodb.BasicDBObject;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;

import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;

public class MongoDocumentTraverser {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentTraverser.class);
    private final MongoDocumentStore mongoStore;
    private boolean disableReadOnlyCheck;

    public static class TraversingRange {

        private final LastModifiedRange lastModifiedRange;
        /**
         * could be null to indicate start from first document in the lastModifiedRange
         */
        private final String startAfterDocumentID;

        public TraversingRange(LastModifiedRange lastModifiedRange, String startAfterDocumentID) {
            this.lastModifiedRange = lastModifiedRange;
            this.startAfterDocumentID = startAfterDocumentID;
        }

        public boolean coversAllDocuments() {
            return lastModifiedRange.coversAllDocuments() && startAfterDocumentID == null;
        }

        public LastModifiedRange getLastModifiedRange() {
            return lastModifiedRange;
        }

        private BsonDocument getFindQuery() {
            String lastModifiedRangeQueryPart = "{$gte:" + lastModifiedRange.getLastModifiedFrom() + ",";
            lastModifiedRangeQueryPart += "$lt:" + lastModifiedRange.getLastModifiedTo() + "}";
            String idRangeQueryPart = "";
            if (startAfterDocumentID != null) {
                String condition = "{$gt:\"" + startAfterDocumentID + "\"}";
                idRangeQueryPart = ", " + NodeDocument.ID + ":" + condition;
            }
            return BsonDocument.parse("{" + NodeDocument.MODIFIED_IN_SECS + ":" + lastModifiedRangeQueryPart
                    + idRangeQueryPart  + "}");
        }

        public String getStartAfterDocumentID() {
            return startAfterDocumentID;
        }

        @Override
        public String toString() {
            return "Range: " + lastModifiedRange.toString() + ", startAfterDocument: " + startAfterDocumentID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TraversingRange that = (TraversingRange) o;
            return Objects.equals(lastModifiedRange, that.lastModifiedRange) && Objects.equals(startAfterDocumentID, that.startAfterDocumentID);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lastModifiedRange, startAfterDocumentID);
        }
    }

    public MongoDocumentTraverser(MongoDocumentStore mongoStore) {
        this.mongoStore = mongoStore;
    }

    public <T extends Document> CloseableIterable<T> getAllDocuments(Collection<T> collection, TraversingRange traversingRange,
                                                                     Predicate<String> filter) {
        if (!disableReadOnlyCheck) {
            checkState(mongoStore.isReadOnly(), "Traverser can only be used with readOnly store");
        }

        MongoCollection<BasicDBObject> dbCollection = mongoStore.getDBCollection(collection);
        //TODO This may lead to reads being routed to secondary depending on MongoURI
        //So caller must ensure that its safe to read from secondary
        Iterable<BasicDBObject> cursor;
        if (traversingRange.coversAllDocuments()) {
            cursor = dbCollection
                    .withReadPreference(mongoStore.getConfiguredReadPreference(collection))
                    .find();
        } else {
            ReadPreference preference = mongoStore.getConfiguredReadPreference(collection);
            LOG.info("Using read preference {}", preference.getName());
            cursor = dbCollection
                    .withReadPreference(preference)
                    .find(traversingRange.getFindQuery()).sort(new BsonDocument()
                            .append(NodeDocument.MODIFIED_IN_SECS, new BsonInt64(1))
                            .append(NodeDocument.ID, new BsonInt64(1)));
        }

        CloseableIterable<BasicDBObject> closeableCursor = CloseableIterable.wrap(cursor);
        cursor = closeableCursor;

        @SuppressWarnings("Guava")
        Iterable<T> result = FluentIterable.from(cursor)
                .filter(o -> filter.test((String) o.get(Document.ID)))
                .transform(o -> {
                    T doc = mongoStore.convertFromDBObject(collection, o);
                    //TODO Review the cache update approach where tracker has to track *all* docs
                    if (collection == Collection.NODES) {
                        NodeDocument nodeDoc = (NodeDocument) doc;
                        getNodeDocCache().put(nodeDoc);
                    }
                    return doc;
                });
        return CloseableIterable.wrap(result, closeableCursor);
    }

    /**
     * For testing only
     */
    void disableReadOnlyCheck() {
        this.disableReadOnlyCheck = true;
    }

    private NodeDocumentCache getNodeDocCache() {
        return mongoStore.getNodeDocumentCache();
    }
}
