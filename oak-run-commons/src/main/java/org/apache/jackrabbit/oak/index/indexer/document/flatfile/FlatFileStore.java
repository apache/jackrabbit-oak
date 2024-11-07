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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createReader;

public class FlatFileStore implements IndexStore {
    private final Closer closer = Closer.create();
    private final BlobStore blobStore;
    private final File storeFile;
    private final File metadataFile;
    private final NodeStateEntryReader entryReader;
    private final Set<String> preferredPathElements;
    private final Compression algorithm;
    private long entryCount = -1;

    private static final String STORE_TYPE = "FlatFileStore";

    public FlatFileStore(BlobStore blobStore, File storeFile, File metadataFile, NodeStateEntryReader entryReader, Set<String> preferredPathElements, Compression algorithm) {
        this.blobStore = blobStore;
        this.storeFile = storeFile;
        if (!(storeFile.exists() && storeFile.isFile() && storeFile.canRead())) {
            String msg = String.format("Cannot read store file at [%s]",
                    storeFile.getAbsolutePath());
            throw new IllegalArgumentException(msg);
        }
        this.entryReader = entryReader;
        this.preferredPathElements = preferredPathElements;
        this.algorithm = algorithm;
        this.metadataFile = metadataFile;
    }

    public FlatFileStore(BlobStore blobStore, File storeFile, NodeStateEntryReader entryReader, Set<String> preferredPathElements, Compression algorithm) {
        this(blobStore, storeFile, null, entryReader, preferredPathElements, algorithm);
    }

    public String getStorePath() {
        return storeFile.getParentFile().getAbsolutePath();
    }

    public File getStoreFile() {
        return storeFile;
    }

    /**
     *
     * @deprecated use {@link #getStorePath()} instead
     */
    @Deprecated
    public String getFlatFileStorePath() {
        return storeFile.getParentFile().getAbsolutePath();
    }

    public long getEntryCount() {
        return entryCount;
    }

    public void setEntryCount(long entryCount) {
        this.entryCount = entryCount;
    }

    public String getMetadataFilePath() {
        return metadataFile != null ? metadataFile.getAbsolutePath() : null;
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        String fileName = new File(storeFile.getParent(), storeFile.getName() + ".linkedList").getAbsolutePath();
        FlatFileStoreIterator it = new FlatFileStoreIterator(blobStore, fileName, createBaseIterator(), preferredPathElements);
        closer.register(it::close);
        return it;
    }

    private Iterator<NodeStateEntry> createBaseIterator() {
        LineIterator itr = new LineIterator(createReader(storeFile, algorithm));
        closer.register(itr::close);
        return new AbstractIterator<NodeStateEntry>() {
            @Override
            protected NodeStateEntry computeNext() {
                if (itr.hasNext()) {
                    return convert(itr.nextLine());
                }

                //End of iterator then close it
                LineIterator.closeQuietly(itr);
                return endOfData();
            }
        };
    }

    private NodeStateEntry convert(String line) {
        return entryReader.read(line);
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    @Override
    public String getIndexStoreType() {
        return STORE_TYPE;
    }

    @Override
    public boolean isIncremental() {
        return false;
    }

    public Compression getAlgorithm() {
        return algorithm;
    }
}
