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
package org.apache.jackrabbit.oak.segment.split;

import com.microsoft.azure.storage.StorageException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static org.junit.Assert.assertEquals;

public class SplitPersistenceTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private SegmentNodeStore base;

    private SegmentNodeStore split;

    private FileStore baseFileStore;

    private FileStore splitFileStore;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException, CommitFailedException, URISyntaxException, InvalidKeyException, StorageException {
        SegmentNodeStorePersistence sharedPersistence = new AzurePersistence(azurite.getContainer("oak-test").getDirectoryReference("oak"));

        baseFileStore = FileStoreBuilder
                .fileStoreBuilder(folder.newFolder())
                .withCustomPersistence(sharedPersistence)
                .build();
        base = SegmentNodeStoreBuilders.builder(baseFileStore).build();

        NodeBuilder builder = base.getRoot().builder();
        builder.child("foo").child("bar").setProperty("version", "v1");
        base.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        baseFileStore.flush();

        SegmentNodeStorePersistence localPersistence = new TarPersistence(folder.newFolder());
        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(sharedPersistence, localPersistence);

        splitFileStore = FileStoreBuilder
                .fileStoreBuilder(folder.newFolder())
                .withCustomPersistence(splitPersistence)
                .build();
        split = SegmentNodeStoreBuilders.builder(splitFileStore).build();
    }

    @After
    public void tearDown() {
        splitFileStore.close();
        baseFileStore.close();
    }

    @Test
    public void testBaseNodeAvailable() {
        assertEquals("v1", split.getRoot().getChildNode("foo").getChildNode("bar").getString("version"));
    }

    @Test
    public void testChangesAreLocalForBaseRepository() throws CommitFailedException {
        NodeBuilder builder = base.getRoot().builder();
        builder.child("foo").child("bar").setProperty("version", "v2");
        base.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals("v1", split.getRoot().getChildNode("foo").getChildNode("bar").getString("version"));
    }

    @Test
    public void testChangesAreLocalForSplitRepository() throws CommitFailedException {
        NodeBuilder builder = split.getRoot().builder();
        builder.child("foo").child("bar").setProperty("version", "v2");
        split.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals("v1", base.getRoot().getChildNode("foo").getChildNode("bar").getString("version"));
    }
}
