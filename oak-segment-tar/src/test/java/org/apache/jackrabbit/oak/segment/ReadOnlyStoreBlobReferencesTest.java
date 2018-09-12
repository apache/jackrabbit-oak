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

package org.apache.jackrabbit.oak.segment;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;

/**
 * Tests for ReadOnlyFileStore#collectReferences
 */
public class ReadOnlyStoreBlobReferencesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));


    @Test
    public void collectReferences()
        throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        File dataStoreDir = new File(getFileStoreFolder(), "blobstore");
        String blobId = createLoad(fileStoreDir, dataStoreDir).getContentIdentity();

        assertReferences(fileStoreDir, dataStoreDir, 1, blobId);
    }

    @Test
    public void collectReferencesAfterGC()
        throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        File dataStoreDir = new File(getFileStoreFolder(), "blobstore");
        String blobId = createLoad(fileStoreDir, dataStoreDir).getContentIdentity();


        try (FileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(newBlobStore(dataStoreDir))
            .withGCOptions(defaultGCOptions().setGcSizeDeltaEstimation(1).setRetainedGenerations(1)).build()) {

            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.removeProperty("bin");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            Set<String> actualReferences = newHashSet();
            fileStore.collectBlobReferences(actualReferences::add);
            assertEquals("Binary should be visible before gc cycle", 1, actualReferences.size());
            assertEquals("Binary reference returned should be same", blobId,
                actualReferences.toArray(new String[0])[0]);

            actualReferences = newHashSet();
            fileStore.fullGC();
            fileStore.collectBlobReferences(actualReferences::add);
            assertEquals("Binary should be deleted after gc cycle", 0, actualReferences.size());
        }

        assertReferences(fileStoreDir, dataStoreDir, 0, null);
    }

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static BlobStore newBlobStore(File directory) {
        OakFileDataStore delegate = new OakFileDataStore();
        delegate.setPath(directory.getAbsolutePath());
        delegate.init(null);
        return new DataStoreBlobStore(delegate);
    }

    private Blob createLoad(File fileStoreDir, File dataStoreDir)
        throws IOException, CommitFailedException, InvalidFileStoreVersionException {
        try (FileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(newBlobStore(dataStoreDir))
            .withGCOptions(defaultGCOptions().setGcSizeDeltaEstimation(0)).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            NodeBuilder builder = nodeStore.getRoot().builder();
            Blob blob = createBlob(nodeStore, 18000);
            builder.setProperty("bin", blob);
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();
            return blob;
        }
    }

    private void assertReferences(File fileStoreDir, File dataStoreDir, int count, String blobId)
        throws IOException, InvalidFileStoreVersionException {
        try (ReadOnlyFileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(newBlobStore(dataStoreDir))
            .buildReadOnly()) {

            Set<String> actualReferences = newHashSet();
            fileStore.collectBlobReferences(actualReferences::add);
            assertEquals("Read only store visible references different", count, actualReferences.size());
            if (!Strings.isNullOrEmpty(blobId)) {
                assertEquals("Binary reference returned should be same", blobId,
                    actualReferences.toArray(new String[0])[0]);
            }
        }
    }
}
