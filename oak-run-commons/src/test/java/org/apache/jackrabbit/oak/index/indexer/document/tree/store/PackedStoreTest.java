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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.FilePacker;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PackedStoreTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void test() throws IOException {
        File dir = temporaryFolder.newFolder("sourceDir");
        Store store = StoreBuilder.build(
                "type=file\n" +
                "dir="+dir.getAbsolutePath()+"\n" +
                "cacheSizeMB=1\n" +
                "maxFileSizeBytes=100");
        store.setWriteCompression(Compression.LZ4);
        store.removeAll();
        TreeSession session = new TreeSession(store);
        session.init();
        int count = 100;
        TreeMap<String, String> verify = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            session.put("hello" + i, "world" + i);
            verify.put("hello" + i, "world" + i);
        }
        session.checkpoint();
        for (int i = 0; i < count; i++) {
            if (i % 3 == 0) {
                session.put("hello" + i, null);
                verify.put("hello" + i, null);
            } else if (i % 3 == 1) {
                session.put("hello" + i, "World" + i);
                verify.put("hello" + i, "World" + i);
            }
        }
        session.flush();
        store.close();

        File pack = temporaryFolder.newFile("pack");
        FilePacker.pack(dir, TreeSession.getFileNameRegex(), pack, true);
        Store packStore = StoreBuilder.build(
                "type=pack\n" +
                "file=" + pack.getAbsolutePath() + "\n" +
                "maxFileSizeBytes=100");
        RandomAccessFile f = new RandomAccessFile(pack, "r");
        ArrayList<FilePacker.FileEntry> list = FilePacker.readDirectoryListing(pack, f);
        assertEquals(152, list.size());
        f.close();
        TreeSession session2 = new TreeSession(packStore);
        for(Entry<String, String> e : verify.entrySet()) {
            String v = session2.get(e.getKey());
            assertEquals(v, e.getValue());
        }
    }
}
