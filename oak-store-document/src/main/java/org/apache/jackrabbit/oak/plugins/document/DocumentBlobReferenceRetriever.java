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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Iterator;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link BlobReferenceRetriever} for the DocumentNodeStore.
 */
public class DocumentBlobReferenceRetriever implements BlobReferenceRetriever {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final DocumentNodeStore nodeStore;

    public DocumentBlobReferenceRetriever(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    @Override
    public void collectReferences(ReferenceCollector collector) {
        int referencesFound = 0;
        Iterator<ReferencedBlob> blobIterator = null;
        try {
            blobIterator = nodeStore.getReferencedBlobsIterator();
            while (blobIterator.hasNext()) {
                ReferencedBlob refBlob = blobIterator.next();
                Blob blob = refBlob.getBlob();
                referencesFound++;

                // TODO this mode would also add in memory blobId
                // Would that be an issue

                if (blob instanceof BlobStoreBlob) {
                    collector.addReference(((BlobStoreBlob) blob).getBlobId(), refBlob.getId());
                } else {
                    // TODO Should not rely on toString. Instead obtain
                    // secure reference and convert that to blobId using
                    // blobStore

                    collector.addReference(blob.toString(), refBlob.getId());
                }
            }
        } finally {
            Utils.closeIfCloseable(blobIterator);
        }
        log.debug("Total blob references found (including chunk resolution) [{}]", referencesFound);
    }
}

