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

import com.mongodb.BasicDBObject;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.guava.common.collect.FluentIterable;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;

public class MongoDocumentTraverser {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentTraverser.class);
    private final MongoDocumentStore mongoStore;
    private boolean disableReadOnlyCheck;

    public MongoDocumentTraverser(MongoDocumentStore mongoStore) {
        this.mongoStore = mongoStore;
    }

    public <T extends Document> CloseableIterable<T> getAllDocuments(Collection<T> collection, TraversingRange traversingRange,
                                                                     Predicate<String> filter) {
        if (!disableReadOnlyCheck) {
            Validate.checkState(mongoStore.isReadOnly(), "Traverser can only be used with readOnly store");
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
