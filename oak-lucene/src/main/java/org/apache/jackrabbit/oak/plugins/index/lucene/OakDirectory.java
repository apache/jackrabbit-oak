/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.benchmark.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * Implementation of the Lucene {@link Directory} (a flat list of files)
 * based on an Oak {@link NodeBuilder}.
 */
public class OakDirectory extends Directory {
    static final PerfLogger PERF_LOGGER = new PerfLogger(LoggerFactory.getLogger(OakDirectory.class.getName() + ".perf"));
    static final String PROP_DIR_LISTING = "dirListing";
    static final String PROP_BLOB_SIZE = "blobSize";
    static final String PROP_UNIQUE_KEY = "uniqueKey";
    static final int UNIQUE_KEY_SIZE = 16;

    private final static SecureRandom secureRandom = new SecureRandom();

    protected final NodeBuilder builder;
    protected final String dataNodeName;
    protected final NodeBuilder directoryBuilder;
    private final IndexDefinition definition;
    private LockFactory lockFactory;
    private final boolean readOnly;
    private final Set<String> fileNames = Sets.newConcurrentHashSet();
    private final Set<String> fileNamesAtStart;
    private final String indexName;
    private final BlobFactory blobFactory;
    private final BlobDeletionCallback blobDeletionCallback;
    private volatile boolean dirty;

    public OakDirectory(NodeBuilder builder, IndexDefinition definition, boolean readOnly) {
        this(builder, INDEX_DATA_CHILD_NAME, definition, readOnly);
    }

    public OakDirectory(NodeBuilder builder, String dataNodeName, IndexDefinition definition, boolean readOnly) {
        this(builder, dataNodeName, definition, readOnly, BlobFactory.getNodeBuilderBlobFactory(builder));
    }

    public OakDirectory(NodeBuilder builder, String dataNodeName, IndexDefinition definition,
                        boolean readOnly, @Nullable GarbageCollectableBlobStore blobStore) {
        this(builder, dataNodeName, definition, readOnly, blobStore, BlobDeletionCallback.NOOP);
    }

    public OakDirectory(NodeBuilder builder, String dataNodeName, IndexDefinition definition,
                        boolean readOnly, @Nullable GarbageCollectableBlobStore blobStore,
                        @Nonnull BlobDeletionCallback blobDeletionCallback) {
        this(builder, dataNodeName, definition, readOnly,
                blobStore != null ? BlobFactory.getBlobStoreBlobFactory(blobStore) : BlobFactory.getNodeBuilderBlobFactory(builder),
                blobDeletionCallback);
    }

    public OakDirectory(NodeBuilder builder, String dataNodeName, IndexDefinition definition,
                        boolean readOnly, BlobFactory blobFactory) {
        this(builder, dataNodeName, definition, readOnly, blobFactory, BlobDeletionCallback.NOOP);
    }

    public OakDirectory(NodeBuilder builder, String dataNodeName, IndexDefinition definition,
                        boolean readOnly, BlobFactory blobFactory,
                        @Nonnull BlobDeletionCallback blobDeletionCallback) {
        this.lockFactory = NoLockFactory.getNoLockFactory();
        this.builder = builder;
        this.dataNodeName = dataNodeName;
        this.directoryBuilder = readOnly ? builder.getChildNode(dataNodeName) : builder.child(dataNodeName);
        this.definition = definition;
        this.readOnly = readOnly;
        this.fileNames.addAll(getListing());
        this.fileNamesAtStart = ImmutableSet.copyOf(this.fileNames);
        this.indexName = definition.getIndexName();
        this.blobFactory = blobFactory;
        this.blobDeletionCallback = blobDeletionCallback;
    }

    @Override
    public String[] listAll() throws IOException {
        return fileNames.toArray(new String[fileNames.size()]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return fileNames.contains(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        checkArgument(!readOnly, "Read only directory");
        fileNames.remove(name);
        NodeBuilder f = directoryBuilder.getChildNode(name);
        PropertyState property = f.getProperty(JCR_DATA);
        if (property != null) {
            if (property.getType() == BINARIES || property.getType() == BINARY) {
                for (Blob b : property.getValue(BINARIES)) {
                    //Mark the blob as deleted. Also, post index path, type of directory
                    //(:suggest, :data, etc) and filename being deleted
                    String blobId = b.getContentIdentity();
                    if (blobId != null) {
                        blobDeletionCallback.deleted(blobId,
                                Lists.newArrayList(definition.getIndexPath(), dataNodeName, name));
                    }
                }
            }
        }
        f.remove();
        markDirty();
    }

    @Override
    public long fileLength(String name) throws IOException {
        NodeBuilder file = directoryBuilder.getChildNode(name);
        if (!file.exists()) {
            String msg = String.format("[%s] %s", indexName, name);
            throw new FileNotFoundException(msg);
        }
        OakIndexInput input = new OakIndexInput(name, file, indexName, blobFactory);
        try {
            return input.length();
        } finally {
            input.close();
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
            throws IOException {
        checkArgument(!readOnly, "Read only directory");
        NodeBuilder file;
        if (!directoryBuilder.hasChildNode(name)) {
            file = directoryBuilder.child(name);
            byte[] uniqueKey = new byte[UNIQUE_KEY_SIZE];
            secureRandom.nextBytes(uniqueKey);
            String key = StringUtils.convertBytesToHex(uniqueKey);
            file.setProperty(PROP_UNIQUE_KEY, key);
            file.setProperty(PROP_BLOB_SIZE, definition.getBlobSize());
        } else {
            file = directoryBuilder.child(name);
        }
        fileNames.add(name);
        markDirty();
        return new OakIndexOutput(name, file, indexName, blobFactory);
    }


    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException {
        NodeBuilder file = directoryBuilder.getChildNode(name);
        if (file.exists()) {
            return new OakIndexInput(name, file, indexName, blobFactory);
        } else {
            String msg = String.format("[%s] %s", indexName, name);
            throw new FileNotFoundException(msg);
        }
    }

    @Override
    public Lock makeLock(String name) {
        return lockFactory.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        lockFactory.clearLock(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // ?
    }

    @Override
    public void close() throws IOException {
        if (!readOnly && definition.saveDirListing()) {
            if (!fileNamesAtStart.equals(fileNames)) {
                directoryBuilder.setProperty(createProperty(PROP_DIR_LISTING, fileNames, STRINGS));
            }
        }
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        this.lockFactory = lockFactory;
    }

    @Override
    public LockFactory getLockFactory() {
        return lockFactory;
    }

    @Override
    public String toString() {
        return "Directory for " + definition.getIndexName();
    }

    /**
     * Copies the file with the given {@code name} to the {@code dest}
     * directory. The file is copied 'by reference'. That is, the file in the
     * destination directory will reference the same blob values as the source
     * file.
     * <p>
     * This method is a no-op if the file does not exist in this directory.
     *
     * @param dest the destination directory.
     * @param name the name of the file to copy.
     * @throws IOException if an error occurs while copying the file.
     * @throws IllegalArgumentException if the destination directory does not
     *          use the same {@link BlobFactory} as {@code this} directory.
     */
    public void copy(OakDirectory dest, String name)
            throws IOException {
        if (blobFactory != dest.blobFactory) {
            throw new IllegalArgumentException("Source and destination " +
                    "directory must reference the same BlobFactory");
        }
        NodeBuilder file = directoryBuilder.getChildNode(name);
        if (file.exists()) {
            // overwrite potentially already existing child
            NodeBuilder destFile = dest.directoryBuilder.setChildNode(name, EMPTY_NODE);
            for (PropertyState p : file.getProperties()) {
                destFile.setProperty(p);
            }
            dest.fileNames.add(name);
            dest.markDirty();
        }
    }

    public boolean isDirty() {
        return dirty;
    }

    private void markDirty() {
        dirty = true;
    }

    private Set<String> getListing(){
        long start = PERF_LOGGER.start();
        Iterable<String> fileNames = null;
        if (definition.saveDirListing()) {
            PropertyState listing = directoryBuilder.getProperty(PROP_DIR_LISTING);
            if (listing != null) {
                fileNames = listing.getValue(Type.STRINGS);
            }
        }

        if (fileNames == null){
            fileNames = directoryBuilder.getChildNodeNames();
        }
        Set<String> result = ImmutableSet.copyOf(fileNames);
        PERF_LOGGER.end(start, 100, "Directory listing performed. Total {} files", result.size());
        return result;
    }

}
