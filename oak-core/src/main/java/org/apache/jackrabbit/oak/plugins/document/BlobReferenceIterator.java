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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.jackrabbit.oak.api.Blob;

/**
 * An iterator over all referenced binaries.
 * <p>
 * Only top-level referenced are returned (indirection, if any, is not resolved).
 * The items are returned in no particular order.
 * An item might be returned multiple times.
 */
public class BlobReferenceIterator implements Iterator<Blob> {

    private static final int BATCH_SIZE = 1000;
    private final DocumentStore docStore;
    private final BlobCollector blobCollector;
    private HashSet<Blob> batch = new HashSet<Blob>();
    private Iterator<Blob> batchIterator;
    private boolean done;
    private String fromKey = NodeDocument.MIN_ID_VALUE;

    public BlobReferenceIterator(DocumentNodeStore nodeStore) {
        this.docStore = nodeStore.getDocumentStore();
        batchIterator = batch.iterator();
        this.blobCollector = new BlobCollector(nodeStore);
    }

    @Override
    public boolean hasNext() {
        if (!batchIterator.hasNext()) {
            loadBatch();
        }
        return batchIterator.hasNext() || !done;
    }

    @Override
    public Blob next() {
        // this will load the next batch if required
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return batchIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void loadBatch() {
        if (done) {
            return;
        }
        batch.clear();
        // read until at least BATCH_SIZE references are available
        while (true) {
            boolean hasMore = loadBatchQuery();
            if (!hasMore) {
                done = true;
                break;
            }
            if (batch.size() > BATCH_SIZE) {
                break;
            }
        }
        batchIterator = batch.iterator();
    }

    private boolean loadBatchQuery() {
        // read about BATCH_SIZE documents
        List<NodeDocument> list =
                docStore.query(Collection.NODES, fromKey, NodeDocument.MAX_ID_VALUE, NodeDocument.HAS_BINARY_FLAG,
                        NodeDocument.HAS_BINARY_VAL,
                        BATCH_SIZE);
        boolean hasMore = false;
        for (NodeDocument doc : list) {
            if (doc.getId().equals(fromKey)) {
                // already read
                continue;
            }
            hasMore = true;
            fromKey = doc.getId();
            blobCollector.collect(doc, batch);
        }
        return hasMore;
    }



}
