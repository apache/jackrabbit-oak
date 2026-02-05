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
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.squeeze;

/**
 * A directory implementation that buffers changes until {@link #close()},
 * except for blob values. Those are written immediately to the store.
 */
public final class BufferedOakDirectory extends Directory {
    public static final String ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM = "oak.lucene.enableSingleBlobIndexFiles";
    private static boolean enableWritingSingleBlobIndexFile = Boolean.parseBoolean(
            System.getProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "true"));
    public static void setEnableWritingSingleBlobIndexFile (boolean val) {
        String cliValStr = System.getProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);

        if (cliValStr != null) {
            boolean cliVal = Boolean.parseBoolean(cliValStr);

            if (cliVal != val) {
                LOG.warn("Ignoring configuration {} as CLI param overrides with a different value", val);
                if (cliVal != enableWritingSingleBlobIndexFile) {
                    enableWritingSingleBlobIndexFile = cliVal;
                }
                return;
            }
        }
        enableWritingSingleBlobIndexFile = val;
    }
    public static boolean isEnableWritingSingleBlobIndexFile() {
        return enableWritingSingleBlobIndexFile;
    }
    // for test
    static void reReadCommandLineParam() {
        String val = System.getProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);
        if (val != null) {
            enableWritingSingleBlobIndexFile = Boolean.parseBoolean(val);
        }
    }

    static final int DELETE_THRESHOLD_UNTIL_REOPEN = 100;

    private static final Logger LOG = LoggerFactory.getLogger(BufferedOakDirectory.class);

    private final BlobFactory blobFactory;

    private final BlobDeletionCallback blobDeletionCallback;

    private final String dataNodeName;

    private final LuceneIndexDefinition definition;

    private final OakDirectory base;

    private final Set<String> bufferedForDelete = SetUtils.newConcurrentHashSet();

    private NodeBuilder bufferedBuilder = EMPTY_NODE.builder();

    private OakDirectory buffered;

    private int deleteCount;


    public BufferedOakDirectory(@NotNull NodeBuilder builder,
                                @NotNull String dataNodeName,
                                @NotNull LuceneIndexDefinition definition,
                                @Nullable BlobStore blobStore) {
        this(builder, dataNodeName, definition, blobStore, BlobDeletionCallback.NOOP);
    }

    public BufferedOakDirectory(@NotNull NodeBuilder builder,
                                @NotNull String dataNodeName,
                                @NotNull LuceneIndexDefinition definition,
                                @Nullable BlobStore blobStore,
                                @NotNull ActiveDeletedBlobCollectorFactory.BlobDeletionCallback blobDeletionCallback) {
        this.blobFactory = blobStore != null ?
                BlobFactory.getBlobStoreBlobFactory(blobStore) :
                BlobFactory.getNodeBuilderBlobFactory(builder);
        this.blobDeletionCallback = blobDeletionCallback;
        this.dataNodeName = requireNonNull(dataNodeName);
        this.definition = requireNonNull(definition);
        this.base = new OakDirectory(requireNonNull(builder), dataNodeName,
                definition, false, blobFactory, blobDeletionCallback, isEnableWritingSingleBlobIndexFile());
        reopenBuffered();
    }

    @Override
    public String[] listAll() throws IOException {
        LOG.debug("[{}]listAll()", definition.getIndexPath());
        Set<String> all = new TreeSet<>();
        all.addAll(asList(base.listAll()));
        all.addAll(asList(buffered.listAll()));
        all.removeAll(bufferedForDelete);
        return all.toArray(new String[all.size()]);
    }

    /**
     * Check if a file exists. This method is no longer part of the Directory interface
     * in Lucene 5.x, but is kept for internal use.
     */
    public boolean fileExists(String name) {
        LOG.debug("[{}]fileExists({})", definition.getIndexPath(), name);
        if (bufferedForDelete.contains(name)) {
            return false;
        }
        return buffered.fileExists(name) || base.fileExists(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        LOG.debug("[{}]deleteFile({})", definition.getIndexPath(), name);
        if (base.fileExists(name)) {
            bufferedForDelete.add(name);
        }
        if (buffered.fileExists(name)) {
            buffered.deleteFile(name);
            fileDeleted();
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        LOG.debug("[{}]fileLength({})", definition.getIndexPath(), name);
        if (bufferedForDelete.contains(name)) {
            String msg = String.format("already deleted: [%s] %s",
                    definition.getIndexPath(), name);
            throw new FileNotFoundException(msg);
        }
        Directory dir = base;
        if (buffered.fileExists(name)) {
            dir = buffered;
        }
        return dir.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
            throws IOException {
        LOG.debug("[{}]createOutput({})", definition.getIndexPath(), name);
        bufferedForDelete.remove(name);
        return buffered.createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        LOG.debug("[{}]sync({})", definition.getIndexPath(), names);
        buffered.sync(names);
        base.sync(names);
    }

    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException {
        LOG.debug("[{}]openInput({})", definition.getIndexPath(), name);
        if (bufferedForDelete.contains(name)) {
            String msg = String.format("already deleted: [%s] %s",
                    definition.getIndexPath(), name);
            throw new FileNotFoundException(msg);
        }
        Directory dir = base;
        if (buffered.fileExists(name)) {
            dir = buffered;
        }
        return dir.openInput(name, context);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        // In Lucene 5.x, obtainLock replaces makeLock/clearLock
        return base.obtainLock(name);
    }

    @Override
    public void close() throws IOException {
        LOG.debug("[{}]close()", definition.getIndexPath());
        buffered.close();
        // Use OakDirectory.copy() which handles read-only destinations properly
        // (Lucene 5.x's copyFrom() calls createOutput() on the destination which fails for read-only)
        for (String name : buffered.listAll()) {
            buffered.copy(base, name);
        }
        // remove files marked as deleted
        for (String name : bufferedForDelete) {
            base.deleteFile(name);
        }
        base.close();
    }

    /**
     * Renames a file. Required by Lucene 5.x Directory API.
     */
    @Override
    public void renameFile(String source, String dest) throws IOException {
        LOG.debug("[{}]renameFile({}, {})", definition.getIndexPath(), source, dest);
        // Check if file is in buffered directory
        if (buffered.fileExists(source)) {
            // Copy to new name and delete old
            base.copyFrom(buffered, source, dest, IOContext.DEFAULT);
            buffered.deleteFile(source);
        } else if (base.fileExists(source)) {
            // For base directory, we need to copy and delete
            // This is not atomic but matches the semantics
            try (IndexInput in = base.openInput(source, IOContext.DEFAULT);
                 IndexOutput out = base.createOutput(dest, IOContext.DEFAULT)) {
                out.copyBytes(in, in.length());
            }
            base.deleteFile(source);
        } else {
            throw new IOException("File not found: " + source);
        }
        bufferedForDelete.remove(dest);
    }

    private void fileDeleted() throws IOException {
        // get rid of non existing files once in a while
        if (++deleteCount >= DELETE_THRESHOLD_UNTIL_REOPEN) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reopen buffered OakDirectory. Current list of files: {}",
                        Arrays.asList(buffered.listAll()));
            }
            buffered.close();
            reopenBuffered();
        }
    }

    private void reopenBuffered() {
        // squeeze out child nodes marked as non existing
        // those are files that were created and later deleted again
        bufferedBuilder = squeeze(bufferedBuilder.getNodeState()).builder();
        buffered = new OakDirectory(bufferedBuilder, dataNodeName,
                definition, false, blobFactory, blobDeletionCallback, isEnableWritingSingleBlobIndexFile());
    }
}
