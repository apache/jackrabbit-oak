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

package org.apache.jackrabbit.oak.segment.migration;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.blob.migration.AbstractMigratorTest;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class SegmentToExternalMigrationTest extends AbstractMigratorTest {

    private FileStore store;

    @Override
    protected NodeStore createNodeStore(BlobStore blobStore, File repository) throws IOException {
        File segmentDir = new File(repository, "segmentstore");
        FileStoreBuilder builder = fileStoreBuilder(segmentDir);
        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }

        try {
            store = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }

        return SegmentNodeStoreBuilders.builder(store).build();
    }

    @Override
    protected void closeNodeStore() {
        store.close();
    }

    @Override
    protected BlobStore createOldBlobStore(File repository) {
        return null;
    }

    @Override
    protected BlobStore createNewBlobStore(File repository) {
        return new FileBlobStore(repository.getPath() + "/new");
    }

}
