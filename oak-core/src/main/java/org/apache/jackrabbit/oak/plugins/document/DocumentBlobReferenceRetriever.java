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
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link BlobReferenceRetriever} for the DocumentNodeStore.
 */
public class DocumentBlobReferenceRetriever implements BlobReferenceRetriever {
    public static final Logger LOG = LoggerFactory.getLogger(DocumentBlobReferenceRetriever.class);

    private final Iterator<Blob> blobIterator;

    public DocumentBlobReferenceRetriever(Iterator<Blob> iterator) {
        this.blobIterator = iterator;
    }

    @Override
    public void getReferences(ReferenceCollector collector) throws Exception {
        int referencesFound = 0;
        while (blobIterator.hasNext()) {
            Blob blob = blobIterator.next();
            if (blob.length() != 0) {
                collector.addReference(blob.toString());
            }
        }

        LOG.debug("Blob references found (including chunk resolution) " + referencesFound);
    }
}

