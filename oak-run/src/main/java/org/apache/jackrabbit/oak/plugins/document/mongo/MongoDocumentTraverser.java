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

import java.util.function.Predicate;

import com.google.common.collect.FluentIterable;
import com.google.common.io.Closer;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;

import static com.google.common.base.Preconditions.checkState;

public class MongoDocumentTraverser {
    private final MongoDocumentStore mongoStore;
    private boolean disableReadOnlyCheck;

    public MongoDocumentTraverser(MongoDocumentStore mongoStore) {
        this.mongoStore = mongoStore;
    }

    public <T extends Document> CloseableIterable<T> getAllDocuments(Collection<T> collection, Predicate<String> filter) {
        if (!disableReadOnlyCheck) {
            checkState(mongoStore.isReadOnly(), "Traverser can only be used with readOnly store");
        }

        DBCollection dbCollection = mongoStore.getDBCollection(collection);
        Closer closer = Closer.create();


        DBCursor cursor = dbCollection.find();
        //TODO This may lead to reads being routed to secondary depending on MongoURI
        //So caller must ensure that its safe to read from secondary
        cursor.setReadPreference(mongoStore.getConfiguredReadPreference(collection));
        closer.register(cursor);

        @SuppressWarnings("Guava")
        Iterable<T> result = FluentIterable.from(cursor)
                .filter(o -> filter.test((String) o.get("_id")))
                .transform(o -> {
                    T doc = mongoStore.convertFromDBObject(collection, o);
                    //TODO Review the cache update approach where tracker has to track *all* docs
                    if (collection == Collection.NODES) {
                        NodeDocument nodeDoc = (NodeDocument) doc;
                        getNodeDocCache().put(nodeDoc);
                    }
                    return doc;
                });
        return CloseableIterable.wrap(result, closer);
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
