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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.Builder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentFactory implements NodeStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentFactory.class);

    private final File dir;

    private final boolean disableMmap;

    public SegmentFactory(String directory, boolean disableMmap, boolean readOnly) {
        this.dir = new File(directory);
        this.disableMmap = disableMmap;
        createDirectoryIfMissing(dir);
        if (readOnly) {
            LOG.info("Read-only segment node store is not supported in 1.0");
        }
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
        File directory = new File(dir, "segmentstore");
        boolean mmapEnabled = !disableMmap && "64".equals(System.getProperty("sun.arch.data.model", "32"));

        final FileStore fs;
        Builder builder;
        builder = FileStore.newFileStore(directory);

        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }
        builder.withMaxFileSize(256);
        builder.withMemoryMapping(mmapEnabled);
        fs = builder.create();

        closer.register(asCloseable(fs));

        return new NodeStoreWithFileStore(new SegmentNodeStore(fs), fs);
    }

    @Override
    public boolean hasExternalBlobReferences() throws IOException {
        FileStore fs = FileStore.newFileStore(new File(dir, "segmentstore")).create();
        try {
            fs.getTracker().collectBlobReferences(new ReferenceCollector() {
                @Override
                public void addReference(String reference) {
                    // FIXME the collector should allow to stop processing
                    // see java.nio.file.FileVisitor
                    throw new ExternalBlobFound();
                }
            });
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
        return String.format("SegmentNodeStore[%s]", dir);
    }

    private static class ExternalBlobFound extends RuntimeException {
    }

    public static class NodeStoreWithFileStore extends ProxyNodeStore {

        private final SegmentNodeStore segmentNodeStore;

        private final FileStore fileStore;

        public NodeStoreWithFileStore(SegmentNodeStore segmentNodeStore, FileStore fileStore) {
            this.segmentNodeStore = segmentNodeStore;
            this.fileStore = fileStore;
        }

        public FileStore getFileStore() {
            return fileStore;
        }

        @Override
        public SegmentNodeStore getNodeStore() {
            return segmentNodeStore;
        }
    }

}
