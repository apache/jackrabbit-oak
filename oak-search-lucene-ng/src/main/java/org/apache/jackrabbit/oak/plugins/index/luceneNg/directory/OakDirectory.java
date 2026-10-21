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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * Lucene 9 Directory implementation that stores index files in Oak repository.
 * Files are stored directly in the {@code storageBuilder} node passed at construction.
 * The caller is responsible for pointing this at the correct storage location
 * (for Lucene 9 Oak indexes, use {@link org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexStorage}).
 * Uses chunked blob storage for memory efficiency.
 */
public class OakDirectory extends Directory {

    static final String PROP_DIR_LISTING = "dirListing";
    static final String PROP_BLOB_SIZE = "blobSize";
    static final String PROP_UNIQUE_KEY = "uniqueKey";
    static final int UNIQUE_KEY_SIZE = 16;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final NodeBuilder storageBuilder;
    private final String indexName;
    private final Set<String> fileNames;
    private final boolean readOnly;
    private final BlobFactory blobFactory;
    private final BlobDeletionCallback blobDeletionCallback;

    /**
     * Creates a new OakDirectory instance.
     * Stores index data directly in {@code storageBuilder} — no child node is created.
     * The caller must pass the correct storage NodeBuilder.
     *
     * @param storageBuilder the NodeBuilder for the directory root
     * @param indexName      the name of the index (used for error messages and temp files)
     * @param readOnly       whether this directory is read-only
     */
    public OakDirectory(NodeBuilder storageBuilder, String indexName, boolean readOnly) {
        this(storageBuilder, indexName, readOnly, BlobDeletionCallback.NOOP);
    }

    public OakDirectory(NodeBuilder storageBuilder, String indexName, boolean readOnly,
                        BlobDeletionCallback blobDeletionCallback) {
        this(storageBuilder, indexName, readOnly,
                BlobFactory.getNodeBuilderBlobFactory(storageBuilder), blobDeletionCallback);
    }

    OakDirectory(NodeBuilder storageBuilder, String indexName, boolean readOnly,
                 BlobFactory blobFactory, BlobDeletionCallback blobDeletionCallback) {
        this.storageBuilder = storageBuilder;
        this.indexName = indexName;
        this.readOnly = readOnly;
        this.blobFactory = blobFactory;
        this.blobDeletionCallback = blobDeletionCallback;

        this.fileNames = SetUtils.newConcurrentHashSet();
        this.fileNames.addAll(getListing());
    }

    @Override
    public String[] listAll() throws IOException {
        return fileNames.toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        checkWritable();
        fileNames.remove(name);
        NodeBuilder file = storageBuilder.getChildNode(name);
        if (file.exists()) {
            notifyBlobDeletion(file, name);
            file.remove();
        }
    }

    private void notifyBlobDeletion(NodeBuilder file, String fileName) {
        PropertyState data = file.getProperty(JCR_DATA);
        if (data == null) {
            return;
        }
        Iterable<String> context = java.util.List.of(indexName, fileName);
        for (Blob blob : data.getValue(Type.BINARIES)) {
            String blobId = blob.getContentIdentity();
            if (blobId != null) {
                blobDeletionCallback.deleted(blobId, context);
            }
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        NodeBuilder file = storageBuilder.getChildNode(name);
        if (!file.exists()) {
            throw new FileNotFoundException(String.format("[%s] %s", indexName, name));
        }
        try (OakIndexInput input = new OakIndexInput(name, file, indexName, blobFactory)) {
            return input.length();
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        checkWritable();

        // Remove existing file if present
        synchronized (storageBuilder) {
            if (storageBuilder.hasChildNode(name)) {
                storageBuilder.getChildNode(name).remove();
            }
        }

        NodeBuilder file = storageBuilder.child(name);
        file.setProperty(PROP_BLOB_SIZE, (long) OakBufferedIndexFile.DEFAULT_BLOB_SIZE);

        byte[] uniqueKey = new byte[UNIQUE_KEY_SIZE];
        SECURE_RANDOM.nextBytes(uniqueKey);
        file.setProperty(PROP_UNIQUE_KEY, StringUtils.convertBytesToHex(uniqueKey));

        fileNames.add(name);
        return new OakIndexOutput(name, file, indexName, blobFactory);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        NodeBuilder file = storageBuilder.getChildNode(name);
        if (!file.exists()) {
            throw new FileNotFoundException(String.format("[%s] %s", indexName, name));
        }
        return new OakIndexInput(name, file, indexName, blobFactory);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        // Oak storage doesn't require locking - return a dummy lock
        return new Lock() {
            @Override
            public void close() throws IOException {
                // No-op
            }

            @Override
            public void ensureValid() throws IOException {
                // No-op
            }
        };
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // No-op for Oak storage
    }

    @Override
    public void close() throws IOException {
        if (!readOnly) {
            storageBuilder.setProperty(createProperty(PROP_DIR_LISTING, fileNames, Type.STRINGS));
        }
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        String name = getTempFileName(prefix, suffix, 0);
        return createOutput(name, context);
    }

    @Override
    public void syncMetaData() throws IOException {
        // No-op for Oak storage
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        checkWritable();
        NodeBuilder sourceFile = storageBuilder.getChildNode(source);
        if (!sourceFile.exists()) {
            throw new FileNotFoundException(String.format("[%s] %s", indexName, source));
        }

        NodeBuilder destFile = storageBuilder.child(dest);
        for (PropertyState prop : sourceFile.getProperties()) {
            destFile.setProperty(prop);
        }

        fileNames.remove(source);
        fileNames.add(dest);

        sourceFile.remove();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Set.of();
    }

    private Set<String> getListing() {
        PropertyState listing = storageBuilder.getProperty(PROP_DIR_LISTING);
        if (listing != null) {
            return SetUtils.toLinkedSet(listing.getValue(Type.STRINGS));
        }
        return SetUtils.toLinkedSet(storageBuilder.getChildNodeNames());
    }

    private void checkWritable() throws IOException {
        if (readOnly) {
            throw new IOException("Directory is read-only");
        }
    }

    private String getTempFileName(String prefix, String suffix, int attempt) {
        return String.format("%s_%s_%d%s", prefix, indexName, System.nanoTime() + attempt, suffix);
    }
}
