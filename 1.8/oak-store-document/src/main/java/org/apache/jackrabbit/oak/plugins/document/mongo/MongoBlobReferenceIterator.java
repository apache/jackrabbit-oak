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

import static com.google.common.collect.Iterables.transform;

import java.util.Iterator;

import org.apache.jackrabbit.oak.plugins.document.BlobReferenceIterator;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;

import com.google.common.base.Function;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class MongoBlobReferenceIterator extends BlobReferenceIterator {

    private final MongoDocumentStore documentStore;

    public MongoBlobReferenceIterator(DocumentNodeStore nodeStore, MongoDocumentStore documentStore) {
        super(nodeStore);
        this.documentStore = documentStore;
    }

    @Override
    public Iterator<NodeDocument> getIteratorOverDocsWithBinaries() {
        DBObject query = QueryBuilder.start(NodeDocument.HAS_BINARY_FLAG).is(NodeDocument.HAS_BINARY_VAL).get();
        // TODO It currently prefers secondary. Would that be Ok?
        DBCursor cursor = documentStore.getDBCollection(Collection.NODES).find(query)
                .setReadPreference(documentStore.getConfiguredReadPreference(Collection.NODES));

        return CloseableIterable.wrap(transform(cursor, new Function<DBObject, NodeDocument>() {
            @Override
            public NodeDocument apply(DBObject input) {
                return documentStore.convertFromDBObject(Collection.NODES, input);
            }
        }), cursor).iterator();

    }
}
