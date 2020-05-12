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
package org.apache.jackrabbit.oak.segment.file.tar;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SegmentTarManagerTest {

    @Rule
    public TemporaryFolder source = new TemporaryFolder(new File("target"));

    @Rule
    public TemporaryFolder destination = new TemporaryFolder(new File("target"));

    private File getSourceFileStoreFolder() {
        return source.getRoot();
    }

    private File getDestinationileStoreFolder() {
        return destination.getRoot();
    }

    protected SegmentNodeStorePersistence getPersistence(File folder) {
        return new TarPersistence(folder);
    }


    @Test
    public void testArchiveRecovery() throws IOException, InvalidFileStoreVersionException, CommitFailedException {

        FileStore store = FileStoreBuilder.fileStoreBuilder(getSourceFileStoreFolder())
                .withStrictVersionCheck(false)
                .withCustomPersistence(getPersistence(getSourceFileStoreFolder())).build();

        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(store).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.close();


        //create second archive
        store = FileStoreBuilder.fileStoreBuilder(getSourceFileStoreFolder())
                .withStrictVersionCheck(false)
                .withCustomPersistence(getPersistence(getSourceFileStoreFolder())).build();

        segmentNodeStore = SegmentNodeStoreBuilders.builder(store).build();
        builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo1", "bar1");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.flush();
        //we will not close second archive now, thus index will not be written


        assertEquals(2, getSourceFileStoreFolder().listFiles((dir, name) -> name.endsWith(".tar")).length);
        assertEquals(0, getSourceFileStoreFolder().listFiles((dir, name) -> name.endsWith(".tar.bak")).length);

        //clone repository data
        FileUtils.copyDirectory(getSourceFileStoreFolder(), getDestinationileStoreFolder());

        // This time, on startup with cloned repository data, recovery will be initiated because previous archive was not closed
        FileStore storeToRecover = FileStoreBuilder.fileStoreBuilder(getDestinationileStoreFolder())
                .withStrictVersionCheck(false)
                .withCustomPersistence(getPersistence(getDestinationileStoreFolder())).build();


        assertEquals(2, getDestinationileStoreFolder().listFiles((dir, name) -> name.endsWith(".tar")).length);
        assertEquals("Backup archive should have been created.", 1, getDestinationileStoreFolder().listFiles((dir, name) -> name.endsWith(".tar.bak")).length);
        segmentNodeStore = SegmentNodeStoreBuilders.builder(storeToRecover).build();

        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        assertEquals("bar1", segmentNodeStore.getRoot().getString("foo1"));

        storeToRecover.close();
        store.close();
    }
}
