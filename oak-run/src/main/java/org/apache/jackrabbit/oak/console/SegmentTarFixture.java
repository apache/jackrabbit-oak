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

package org.apache.jackrabbit.oak.console;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.AbstractFileStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class SegmentTarFixture implements NodeStoreFixture {

    static NodeStoreFixture create(File path, boolean readOnly, BlobStore blobStore) throws IOException {
        FileStoreBuilder builder = fileStoreBuilder(path).withMaxFileSize(256);

        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }

        try {
            if (readOnly) {
                return new SegmentTarFixture(builder.buildReadOnly());
            } else {
                return new SegmentTarFixture(builder.build());
            }
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    private final AbstractFileStore fileStore;

    private final SegmentNodeStore segmentNodeStore;

    private SegmentTarFixture(FileStore fileStore) {
        this.fileStore = fileStore;
        this.segmentNodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    private SegmentTarFixture(ReadOnlyFileStore fileStore) {
        this.fileStore = fileStore;
        this.segmentNodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    @Override
    public NodeStore getStore() {
        return segmentNodeStore;
    }

    @Override
    public void close() throws IOException {
        fileStore.close();
    }

}
