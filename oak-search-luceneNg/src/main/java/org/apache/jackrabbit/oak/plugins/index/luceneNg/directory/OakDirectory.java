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
import java.util.Collection;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * Lucene 9 Directory implementation that stores index files in Oak repository.
 * Files are stored under /var/indexing/lucene/{indexName}/ node structure.
 * Uses chunked blob storage for memory efficiency.
 */
public class OakDirectory extends Directory {

    static final String PROP_DIR_LISTING = "dirListing";
    static final String PROP_BLOB_SIZE = "blobSize";

    private static final String VAR_NODE = "var";
    private static final String INDEXING_NODE = "indexing";
    private static final String LUCENE_NODE = "lucene";

    private final NodeBuilder root;
    private final NodeBuilder directoryBuilder;
    private final String indexName;
    private final Set<String> fileNames;
    private final boolean readOnly;
    private final BlobFactory blobFactory;

    /**
     * Creates a new OakDirectory instance.
     *
     * @param root the root node builder
     * @param indexName the name of the index
     * @param readOnly whether this directory is read-only
     */
    public OakDirectory(NodeBuilder root, String indexName, boolean readOnly) {
        this.root = root;
        this.indexName = indexName;
        this.readOnly = readOnly;
        this.blobFactory = BlobFactory.getNodeBuilderBlobFactory(root);

        // Auto-create /var/indexing/lucene/{indexName} structure
        NodeBuilder var = root.child(VAR_NODE);
        NodeBuilder indexing = var.child(INDEXING_NODE);
        NodeBuilder lucene = indexing.child(LUCENE_NODE);

        if (readOnly) {
            this.directoryBuilder = lucene.getChildNode(indexName);
        } else {
            this.directoryBuilder = lucene.child(indexName);
        }

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
        NodeBuilder file = directoryBuilder.getChildNode(name);
        if (file.exists()) {
            file.remove();
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        NodeBuilder file = directoryBuilder.getChildNode(name);
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
        synchronized (directoryBuilder) {
            if (directoryBuilder.hasChildNode(name)) {
                directoryBuilder.getChildNode(name).remove();
            }
        }

        NodeBuilder file = directoryBuilder.child(name);
        // Set blob size (chunk size)
        file.setProperty(PROP_BLOB_SIZE, (long) OakBufferedIndexFile.DEFAULT_BLOB_SIZE);

        fileNames.add(name);
        return new OakIndexOutput(name, file, indexName, blobFactory);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        String name = getTempFileName(prefix, suffix, 0);
        return createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // No-op for Oak storage
    }

    @Override
    public void syncMetaData() throws IOException {
        // No-op for Oak storage
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        checkWritable();
        NodeBuilder sourceFile = directoryBuilder.getChildNode(source);
        if (!sourceFile.exists()) {
            throw new FileNotFoundException(String.format("[%s] %s", indexName, source));
        }

        // Copy properties to destination
        NodeBuilder destFile = directoryBuilder.child(dest);
        for (PropertyState prop : sourceFile.getProperties()) {
            destFile.setProperty(prop);
        }

        // Update file listing
        fileNames.remove(source);
        fileNames.add(dest);

        // Remove source
        sourceFile.remove();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        NodeBuilder file = directoryBuilder.getChildNode(name);
        if (!file.exists()) {
            throw new FileNotFoundException(String.format("[%s] %s", indexName, name));
        }
        return new OakIndexInput(name, file, indexName, blobFactory);
    }

    @Override
    public void close() throws IOException {
        if (!readOnly) {
            // Save directory listing
            directoryBuilder.setProperty(createProperty(PROP_DIR_LISTING, fileNames, Type.STRINGS));
        }
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Set.of();
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

    private Set<String> getListing() {
        PropertyState listing = directoryBuilder.getProperty(PROP_DIR_LISTING);
        if (listing != null) {
            return SetUtils.toLinkedSet(listing.getValue(Type.STRINGS));
        }
        return SetUtils.toLinkedSet(directoryBuilder.getChildNodeNames());
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
