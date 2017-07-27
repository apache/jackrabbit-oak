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

package org.apache.jackrabbit.oak.backup;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.backup.impl.FileStoreBackupImpl;
import org.apache.jackrabbit.oak.backup.impl.FileStoreRestoreImpl;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileStoreBackupTest {

    private File src;
    private File destination;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void before() throws Exception {
        src = folder.newFolder("src");
        destination = folder.newFolder("dst");
    }

    @Test
    public void testBackup() throws Exception {
        FileStore source = newFileStore(src);
        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(source)
                .build();

        FileStoreBackup fsb = new FileStoreBackupImpl();

        try {
            init(store);
            source.flush();
            fsb.backup(source.getReader(), source.getRevisions(), destination);
            compare(source, destination);

            addTestContent(store);
            source.flush();
            fsb.backup(source.getReader(), source.getRevisions(), destination);
            compare(source, destination);

            source.compactFull();
            fsb.cleanup(source);
            fsb.backup(source.getReader(), source.getRevisions(), destination);
            compare(source, destination);
        } finally {
            source.close();
        }
    }

    @Test
    public void testRestore() throws Exception {
        FileStore source = newFileStore(src);
        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(source)
                .build();
        FileStoreBackup fsb = new FileStoreBackupImpl();
        FileStoreRestore fsr = new FileStoreRestoreImpl();

        init(store);
        source.flush();
        fsb.backup(source.getReader(), source.getRevisions(), destination);
        source.close();

        fsr.restore(destination, src);
        source = newFileStore(src);
        compare(source, destination);
        source.close();
    }

    private static void addTestContent(NodeStore store)
            throws CommitFailedException, IOException {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder c = builder.child("test-backup").child("binaries");
        for (int i = 0; i < 2; i++) {
            c.setProperty("binS" + i, createBlob(store, 6 * 1024));
        }

        for (int i = 0; i < 2; i++) {
            c.setProperty("binL" + i, createBlob(store, 64 * 1024));
        }
        builder.child("root"); // make sure we don't backup the super-root
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static Blob createBlob(NodeStore nodeStore, int size)
            throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static FileStore newFileStore(File fs) throws Exception {
        return fileStoreBuilder(fs)
                .withMaxFileSize(1)
                .withGCOptions(SegmentGCOptions.defaultGCOptions().setOffline())
                .build();
    }

    private static void compare(FileStore store, File destination) throws Exception {
        FileStore backup = fileStoreBuilder(destination).build();
        assertEquals(store.getHead(), backup.getHead());
        backup.close();
    }

    private static void init(NodeStore store) {
        new Oak(store).with(new OpenSecurityProvider())
                .with(new InitialContent()).createContentRepository();
    }

}
