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
package org.apache.jackrabbit.oak.segment.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

public class AzureArchiveManagerTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
    }

    @Test
    public void testRecovery() throws StorageException, URISyntaxException, IOException {
        SegmentArchiveManager manager = new AzurePersistence(container.getDirectoryReference("oak")).createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        container.getBlockBlobReference("oak/data00000a.tar/0005." + uuids.get(5).toString()).delete();

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);
        assertEquals(uuids.subList(0, 5), newArrayList(recovered.keySet()));
    }

    @Test
    public void testUncleanStop() throws URISyntaxException, IOException, InvalidFileStoreVersionException, CommitFailedException, StorageException {
        AzurePersistence p = new AzurePersistence(container.getDirectoryReference("oak"));
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        container.getBlockBlobReference("oak/data00000a.tar/closed").delete();
        container.getBlockBlobReference("oak/data00000a.tar/data00000a.tar.brf").delete();
        container.getBlockBlobReference("oak/data00000a.tar/data00000a.tar.gph").delete();

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }
}
