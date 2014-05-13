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

import java.io.Closeable;
import java.util.Queue;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Queues;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.document.BlobCollector;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;

public class MongoBlobReferenceIterator extends AbstractIterator<Blob> implements Closeable {
    private final MongoDocumentStore documentStore;
    private final BlobCollector blobCollector;
    private final Queue<Blob> blobs = Queues.newArrayDeque();

    private DBCursor cursor;

    public MongoBlobReferenceIterator(DocumentNodeStore nodeStore,
                                      MongoDocumentStore documentStore) {
        this.documentStore = documentStore;
        this.blobCollector = new BlobCollector(nodeStore);
    }

    @Override
    protected Blob computeNext() {
        if (blobs.isEmpty()) {
            loadBatch();
        }
        if (!blobs.isEmpty()) {
            return blobs.remove();
        }
        return endOfData();
    }

    private void loadBatch() {
        initializeCursor();
        //Some node which have the '_bin' flag set might not have any binaries in it
        //so move forward if blobs is still empty and cursor has more elements
        while (cursor.hasNext() && blobs.isEmpty()) {
            collectBinaries(documentStore.convertFromDBObject(Collection.NODES, cursor.next()));
        }
    }

    private void collectBinaries(NodeDocument nodeDocument) {
        blobCollector.collect(nodeDocument, blobs);
    }

    private void initializeCursor() {
        if (cursor == null) {
            DBObject query = QueryBuilder.start(NodeDocument.HAS_BINARY_FLAG)
                    .is(NodeDocument.HAS_BINARY_VAL)
                    .get();
            //TODO It currently prefers secondary. Would that be Ok?
            cursor = getNodeCollection().find(query)
                    .setReadPreference(documentStore.getConfiguredReadPreference(Collection.NODES));
        }
    }

    private DBCollection getNodeCollection() {
        return documentStore.getDBCollection(Collection.NODES);
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
        }
    }
}
