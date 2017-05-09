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

import java.io.Closeable;
import java.util.Iterator;
import java.util.Queue;

import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Queues;

/**
 * An iterator over all referenced binaries.
 * <p>
 * Only top-level referenced are returned (indirection, if any, is not
 * resolved). The items are returned in no particular order. An item might be
 * returned multiple times.
 */
public class BlobReferenceIterator extends AbstractIterator<ReferencedBlob> implements Closeable {

    private final DocumentStore documentStore;
    private final BlobCollector blobCollector;
    private final Queue<ReferencedBlob> blobs = Queues.newArrayDeque();

    private Iterator<NodeDocument> iterator;

    public BlobReferenceIterator(DocumentNodeStore nodeStore) {
        this.documentStore = nodeStore.getDocumentStore();
        this.blobCollector = new BlobCollector(nodeStore);
    }

    @Override
    protected ReferencedBlob computeNext() {
        if (blobs.isEmpty()) {
            loadBatch();
        }

        if (!blobs.isEmpty()) {
            return blobs.remove();
        } else {
            return endOfData();
        }
    }

    private void loadBatch() {
        if (this.iterator == null) {
            this.iterator = getIteratorOverDocsWithBinaries();
        }
        // Some node which have the '_bin' flag set might not have any binaries
        // in it so move forward if blobs is still empty and cursor has more
        // elements
        while (iterator.hasNext() && blobs.isEmpty()) {
            collectBinaries(iterator.next());
        }
    }

    private void collectBinaries(NodeDocument nodeDocument) {
        blobCollector.collect(nodeDocument, blobs);
    }

    /**
     * Override this document to use a document store specific iterator.
     */
    public Iterator<NodeDocument> getIteratorOverDocsWithBinaries() {
        int batchSize = 1000;
        return Utils.getSelectedDocuments(documentStore, NodeDocument.HAS_BINARY_FLAG, NodeDocument.HAS_BINARY_VAL, batchSize)
                .iterator();
    }

    @Override
    public void close() {
        Utils.closeIfCloseable(iterator);
    }
}
