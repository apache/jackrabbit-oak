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

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.index.lucene.BlobFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.squeeze;

/**
 * A directory implementation that buffers changes until {@link #close()},
 * except for blob values. Those are written immediately to the store.
 */
public final class BufferedOakDirectory extends Directory {

    static final int DELETE_THRESHOLD_UNTIL_REOPEN = 100;

    private static final Logger LOG = LoggerFactory.getLogger(BufferedOakDirectory.class);

    private final BlobFactory blobFactory;

    private final BlobDeletionCallback blobDeletionCallback;

    private final String dataNodeName;

    private final IndexDefinition definition;

    private final OakDirectory base;

    private final Set<String> bufferedForDelete = Sets.newConcurrentHashSet();

    private NodeBuilder bufferedBuilder = EMPTY_NODE.builder();

    private OakDirectory buffered;

    private int deleteCount;


    public BufferedOakDirectory(@Nonnull NodeBuilder builder,
                                @Nonnull String dataNodeName,
                                @Nonnull IndexDefinition definition,
                                @Nullable BlobStore blobStore) {
        this(builder, dataNodeName, definition, blobStore, BlobDeletionCallback.NOOP);
    }

    public BufferedOakDirectory(@Nonnull NodeBuilder builder,
                                @Nonnull String dataNodeName,
                                @Nonnull IndexDefinition definition,
                                @Nullable BlobStore blobStore,
                                @Nonnull BlobDeletionCallback blobDeletionCallback) {
        this.blobFactory = blobStore != null ?
                BlobFactory.getBlobStoreBlobFactory(blobStore) :
                BlobFactory.getNodeBuilderBlobFactory(builder);
        this.blobDeletionCallback = blobDeletionCallback;
        this.dataNodeName = checkNotNull(dataNodeName);
        this.definition = checkNotNull(definition);
        this.base = new OakDirectory(checkNotNull(builder), dataNodeName,
                definition, false, blobFactory, blobDeletionCallback);
        reopenBuffered();
    }

    @Override
    public String[] listAll() throws IOException {
        LOG.debug("[{}]listAll()", definition.getIndexPath());
        Set<String> all = Sets.newTreeSet();
        all.addAll(asList(base.listAll()));
        all.addAll(asList(buffered.listAll()));
        all.removeAll(bufferedForDelete);
        return all.toArray(new String[all.size()]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
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
    public Lock makeLock(String name) {
        return base.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        base.clearLock(name);
    }

    @Override
    public void close() throws IOException {
        LOG.debug("[{}]close()", definition.getIndexPath());
        buffered.close();
        // copy buffered files to base
        for (String name : buffered.listAll()) {
            buffered.copy(base, name);
        }
        // remove files marked as deleted
        for (String name : bufferedForDelete) {
            base.deleteFile(name);
        }
        base.close();
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        base.setLockFactory(lockFactory);
    }

    @Override
    public LockFactory getLockFactory() {
        return base.getLockFactory();
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
                definition, false, blobFactory, blobDeletionCallback);
    }
}
