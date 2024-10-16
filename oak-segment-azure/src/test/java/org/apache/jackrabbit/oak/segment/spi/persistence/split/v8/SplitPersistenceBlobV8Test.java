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
package org.apache.jackrabbit.oak.segment.spi.persistence.split.v8;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.microsoft.azure.storage.StorageException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.split.SplitPersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class SplitPersistenceBlobV8Test {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private SegmentNodeStore base;

    private SegmentNodeStore split;

    private FileStore baseFileStore;

    private FileStore splitFileStore;

    private String baseBlobId;

    private SegmentNodeStorePersistence splitPersistence;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException, CommitFailedException, URISyntaxException, InvalidKeyException, StorageException {
        SegmentNodeStorePersistence sharedPersistence =
            new AzurePersistenceV8(azurite.getContainer("oak-test").getDirectoryReference("oak"));
        File dataStoreDir = new File(folder.getRoot(), "blobstore");
        BlobStore blobStore = newBlobStore(dataStoreDir);

        baseFileStore = FileStoreBuilder
                .fileStoreBuilder(folder.newFolder())
                .withCustomPersistence(sharedPersistence)
                .withBlobStore(blobStore)
                .build();
        base = SegmentNodeStoreBuilders.builder(baseFileStore).build();

        NodeBuilder builder = base.getRoot().builder();
        builder.child("foo").child("bar").setProperty("version", "v1");
        base.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        baseBlobId = createLoad(base, baseFileStore).getContentIdentity();
        baseFileStore.flush();
        baseFileStore.close();

        baseFileStore = FileStoreBuilder
            .fileStoreBuilder(folder.newFolder())
            .withCustomPersistence(sharedPersistence)
            .withBlobStore(blobStore)
            .build();
        base = SegmentNodeStoreBuilders.builder(baseFileStore).build();

        createLoad(base, baseFileStore).getContentIdentity();
        baseFileStore.flush();

        SegmentNodeStorePersistence localPersistence = new TarPersistence(folder.newFolder());
        splitPersistence = new SplitPersistence(sharedPersistence, localPersistence);

        splitFileStore = FileStoreBuilder
            .fileStoreBuilder(folder.newFolder())
            .withCustomPersistence(splitPersistence)
            .withBlobStore(blobStore)
            .build();
        split = SegmentNodeStoreBuilders.builder(splitFileStore).build();
    }

    @After
    public void tearDown() {
        baseFileStore.close();
    }

    @Test
    public void collectReferences()
        throws IOException, CommitFailedException {
        String blobId = createLoad(split, splitFileStore).getContentIdentity();

        assertReferences(2, CollectionUtils.toSet(baseBlobId, blobId));
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

    private Blob createLoad(SegmentNodeStore store, FileStore fileStore)
        throws IOException, CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        Blob blob = createBlob(store, 18000);
        builder.setProperty("bin", blob);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fileStore.flush();
        return blob;
    }

    private void assertReferences(int count, Set<String> blobIds)
        throws IOException {
        Set<String> actualReferences = new HashSet<>();
        splitFileStore.collectBlobReferences(actualReferences::add);
        assertEquals("visible references different", count, actualReferences.size());
        assertEquals("Binary reference returned should be same", blobIds, actualReferences);
    }
}
