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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils.asCloseable;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils.NodeStoreWithFileStore;

import com.google.common.io.Closer;

public class SegmentTarFactory implements NodeStoreFactory {

    private final File dir;

    private final boolean disableMmap;

    private int segmentCacheSize;

    private final boolean readOnly;

    public SegmentTarFactory(String directory, boolean disableMmap, int segmentCacheSize, boolean readOnly) {
        this.dir = new File(directory);
        this.disableMmap = disableMmap;
        this.segmentCacheSize = segmentCacheSize;
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
                builder.withSegmentCacheSize(segmentCacheSize > 0 ? segmentCacheSize : DEFAULT_SEGMENT_CACHE_MB);
                fs = builder.buildReadOnly();
                closer.register(asCloseable(fs));
                return SegmentNodeStoreBuilders.builder(fs).build();
            } else {
                final FileStore fs;
                fs = builder.build();
                closer.register(asCloseable(fs));
                return new NodeStoreWithFileStore(SegmentNodeStoreBuilders.builder(fs).build(), fs);
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
        return FileStoreUtils.hasExternalBlobReferences(fs);
    }

    public File getRepositoryDir() {
        return dir;
    }

    @Override
    public String toString() {
        return String.format("SegmentTarNodeStore[%s]", dir);
    }
}
