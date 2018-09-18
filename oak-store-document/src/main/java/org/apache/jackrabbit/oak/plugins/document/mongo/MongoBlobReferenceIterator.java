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

import static com.google.common.collect.Iterators.transform;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

import java.util.Iterator;

import org.apache.jackrabbit.oak.plugins.document.BlobReferenceIterator;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterator;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

public class MongoBlobReferenceIterator extends BlobReferenceIterator {

    private final MongoDocumentStore documentStore;

    public MongoBlobReferenceIterator(DocumentNodeStore nodeStore,
                                      MongoDocumentStore documentStore) {
        super(nodeStore);
        this.documentStore = documentStore;
    }

    @Override
    public Iterator<NodeDocument> getIteratorOverDocsWithBinaries() {
        Bson query = Filters.eq(NodeDocument.HAS_BINARY_FLAG, NodeDocument.HAS_BINARY_VAL);
        // TODO It currently uses the configured read preference. Would that be Ok?
        MongoCursor<BasicDBObject> cursor = documentStore.getDBCollection(NODES)
                .find(query).iterator();

        return CloseableIterator.wrap(transform(cursor,
                input -> documentStore.convertFromDBObject(NODES, input)),
                cursor);
    }
}
