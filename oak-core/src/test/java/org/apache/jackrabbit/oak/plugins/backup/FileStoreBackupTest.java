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
package org.apache.jackrabbit.oak.plugins.backup;

import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class FileStoreBackupTest {

    private File src;
    private File destination;

    @Before
    public void before() {
        long run = System.currentTimeMillis();
        File root = new File("target");
        src = new File(root, "tar-src-" + run);
        destination = new File(root, "tar-dest-" + run);
    }

    @After
    public void after() {
        deleteQuietly(src);
        deleteQuietly(destination);
    }

    @Test
    public void testBackup() throws Exception {
        FileStore source = new FileStore(src, 256, false);

        NodeStore store = new SegmentNodeStore(source);
        init(store);

        // initial content
        FileStoreBackup.backup(store, destination);

        compare(store, destination);

        addTestContent(store);
        FileStoreBackup.backup(store, destination);
        compare(store, destination);

        source.close();
    }

    @Test
    public void testRestore() throws Exception {
        FileStore source = new FileStore(src, 256, false);

        NodeStore store = new SegmentNodeStore(source);
        init(store);

        // initial content
        FileStoreBackup.backup(store, destination);

        addTestContent(store);

        FileStoreRestore.restore(destination, store);

        compare(store, destination);

        source.close();
    }

    private static void addTestContent(NodeStore store)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test-backup");
        builder.child("root"); // make sure we don't backup the super-root
        store.merge(builder, EmptyHook.INSTANCE, null);
    }

    private static void compare(NodeStore store, File destination)
            throws IOException {
        FileStore backup = new FileStore(destination, 256, false);
        assertEquals(store.getRoot(), new SegmentNodeStore(backup).getRoot());
        backup.close();
    }

    private static void init(NodeStore store) {
        new Oak(store).with(new OpenSecurityProvider())
                .with(new InitialContent()).createContentRepository();
    }

    @Test @Ignore("OAK-1159 duplicate content")
    public void testSharedContent() throws Exception {
        FileStore source = new FileStore(src, 256, false);

        NodeStore store = new SegmentNodeStore(source);

        // ~100k
        Blob blob = store.createBlob(new ByteArrayInputStream(new byte[100000]));

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder c1 = builder.child("test-backup");
        c1.setProperty("blob", blob);
        NodeBuilder c2 = builder.child("test-backup2");
        c2.setProperty("blob", blob);
        store.merge(builder, EmptyHook.INSTANCE, null);

        FileStoreBackup.backup(store, destination);
        compare(store, destination);
        source.close();

        Map<String, Long> expected = new HashMap<String, Long>();
        for (File f : src.listFiles()) {
            if (f.getName().endsWith(".tar")) {
                expected.put(f.getName(), f.length());
            }
        }

        for (File f : destination.listFiles()) {
            if(!f.getName().endsWith(".tar")){
                continue;
            }
            assertTrue(f.getName() + " is missing from the backup",
                    expected.containsKey(f.getName()));
            assertTrue(
                    f.getName() + " is expected to have size <= "
                            + expected.get(f.getName()) + " actually is "
                            + f.length(),
                    f.length() <= expected.get(f.getName()));
        }

    }
}
