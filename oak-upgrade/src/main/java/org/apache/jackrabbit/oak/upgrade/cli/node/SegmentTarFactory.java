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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class SegmentTarFactory implements NodeStoreFactory {

    private final File dir;

    private final boolean disableMmap;

    private final boolean readOnly;

    public SegmentTarFactory(String directory, boolean disableMmap, boolean readOnly) {
        this.dir = new File(directory);
        this.disableMmap = disableMmap;
        this.readOnly = readOnly;
        createDirectoryIfMissing(dir);
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dir.getPath());
        }
    }

    private void createDirectoryIfMissing(File directory) {
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    @Override
    public NodeStore create(BlobStore blobStore, Closer closer) throws IOException {
        final FileStoreBuilder builder = fileStoreBuilder(new File(dir, "segmentstore"));
        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }
        builder.withMaxFileSize(256);
        if (disableMmap) {
            builder.withMemoryMapping(false);
        } else {
            builder.withDefaultMemoryMapping();
        }

        try {
            if (readOnly) {
                final ReadOnlyFileStore fs;
                fs = builder.buildReadOnly();
                closer.register(asCloseable(fs));
                return new TarNodeStore(SegmentNodeStoreBuilders.builder(fs).build(), new SegmentTarSuperRootProvider(fs));
            } else {
                final FileStore fs;
                fs = builder.build();
                closer.register(asCloseable(fs));
                return new TarNodeStore(SegmentNodeStoreBuilders.builder(fs).build(), new SegmentTarSuperRootProvider(fs));
            }
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean hasExternalBlobReferences() throws IOException {
        final FileStoreBuilder builder = fileStoreBuilder(new File(dir, "segmentstore"));
        builder.withMaxFileSize(256);
        builder.withMemoryMapping(false);
        ReadOnlyFileStore fs;
        try {
            fs = builder.buildReadOnly();
        } catch (InvalidFileStoreVersionException e) {
            throw new IOException(e);
        }
        try {
            for (SegmentId id : fs.getSegmentIds()) {
                if (!id.isDataSegmentId()) {
                    continue;
                }
                id.getSegment().forEachRecord(new Segment.RecordConsumer() {
                    @Override
                    public void consume(int number, RecordType type, int offset) {
                        // FIXME the consumer should allow to stop processing
                        // see java.nio.file.FileVisitor
                        if (type == RecordType.BLOB_ID) {
                            throw new ExternalBlobFound();
                        }
                    }
                });
            }
            return false;
        } catch (ExternalBlobFound e) {
            return true;
        } finally {
            fs.close();
        }
    }

    public File getRepositoryDir() {
        return dir;
    }

    private static Closeable asCloseable(final ReadOnlyFileStore fs) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                fs.close();
            }
        };
    }

    private static Closeable asCloseable(final FileStore fs) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                fs.close();
            }
        };
    }

    @Override
    public String toString() {
        return String.format("SegmentTarNodeStore[%s]", dir);
    }

    private static class ExternalBlobFound extends RuntimeException {
    }

    private static class SegmentTarSuperRootProvider implements TarNodeStore.SuperRootProvider {

        private final ReadOnlyFileStore readOnlyFileStore;

        private final FileStore fileStore;

        public SegmentTarSuperRootProvider(ReadOnlyFileStore readOnlyFileStore) {
            this.readOnlyFileStore = readOnlyFileStore;
            this.fileStore = null;
        }

        public SegmentTarSuperRootProvider(FileStore fileStore) {
            this.readOnlyFileStore = null;
            this.fileStore = fileStore;
        }

        @Override
        public void setSuperRoot(NodeBuilder builder) {
            if (fileStore == null) {
                throw new IllegalStateException("setSuperRoot is not supported for read-only segment-tar");
            }
            checkArgument(builder instanceof SegmentNodeBuilder);
            SegmentNodeBuilder segmentBuilder = (SegmentNodeBuilder) builder;
            SegmentNodeState lastRoot = (SegmentNodeState) getSuperRoot();

            if (!lastRoot.getRecordId().equals(((SegmentNodeState) segmentBuilder.getBaseState()).getRecordId())) {
                throw new IllegalArgumentException("The new head is out of date");
            }
            fileStore.getRevisions().setHead(lastRoot.getRecordId(), segmentBuilder.getNodeState().getRecordId());
        }

        @Override
        public NodeState getSuperRoot() {
            return fileStore == null ? readOnlyFileStore.getHead() : fileStore.getHead();
        }
    }
}
