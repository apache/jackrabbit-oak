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

    private static final String INDEX_DATA_CHILD_NAME = ":data";

    private final NodeBuilder definitionBuilder;
    private final String indexName;
    private final Set<String> fileNames;
    private final boolean readOnly;
    private final BlobFactory blobFactory;

    /**
     * Creates a new OakDirectory instance.
     * Stores index data under the definition node at :data child node,
     * following the same pattern as legacy Lucene.
     *
     * @param definitionBuilder the index definition node builder
     * @param indexName the name of the index
     * @param readOnly whether this directory is read-only
     */
    public OakDirectory(NodeBuilder definitionBuilder, String indexName, boolean readOnly) {
        this.definitionBuilder = definitionBuilder;
        this.indexName = indexName;
        this.readOnly = readOnly;
        this.blobFactory = BlobFactory.getNodeBuilderBlobFactory(definitionBuilder);

        // Store index data under :data child node of the index definition
        // This follows the same pattern as legacy Lucene
        // We get the directory builder dynamically to avoid staleness issues
        if (!readOnly) {
            // Ensure :data node exists for write mode
            definitionBuilder.child(INDEX_DATA_CHILD_NAME);
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
        NodeBuilder file = getDirectoryBuilder().getChildNode(name);
        if (file.exists()) {
            file.remove();
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        NodeBuilder file = getDirectoryBuilder().getChildNode(name);
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

        NodeBuilder dirBuilder = getDirectoryBuilder();

        // Remove existing file if present
        synchronized (definitionBuilder) {
            if (dirBuilder.hasChildNode(name)) {
                dirBuilder.getChildNode(name).remove();
            }
        }

        NodeBuilder file = dirBuilder.child(name);
        // Set blob size (chunk size)
        file.setProperty(PROP_BLOB_SIZE, (long) OakBufferedIndexFile.DEFAULT_BLOB_SIZE);

        fileNames.add(name);
        return new OakIndexOutput(name, file, indexName, blobFactory);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        NodeBuilder file = getDirectoryBuilder().getChildNode(name);
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
            // Save directory listing
            getDirectoryBuilder().setProperty(createProperty(PROP_DIR_LISTING, fileNames, Type.STRINGS));
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
        NodeBuilder sourceFile = getDirectoryBuilder().getChildNode(source);
        if (!sourceFile.exists()) {
            throw new FileNotFoundException(String.format("[%s] %s", indexName, source));
        }

        // Copy properties to destination
        NodeBuilder destFile = getDirectoryBuilder().child(dest);
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
    public Set<String> getPendingDeletions() throws IOException {
        return Set.of();
    }

    /**
     * Gets the directory builder dynamically to avoid staleness issues.
     */
    private NodeBuilder getDirectoryBuilder() {
        if (readOnly) {
            return definitionBuilder.getChildNode(INDEX_DATA_CHILD_NAME);
        } else {
            return definitionBuilder.child(INDEX_DATA_CHILD_NAME);
        }
    }

    private Set<String> getListing() {
        PropertyState listing = getDirectoryBuilder().getProperty(PROP_DIR_LISTING);
        if (listing != null) {
            return SetUtils.toLinkedSet(listing.getValue(Type.STRINGS));
        }
        return SetUtils.toLinkedSet(getDirectoryBuilder().getChildNodeNames());
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
