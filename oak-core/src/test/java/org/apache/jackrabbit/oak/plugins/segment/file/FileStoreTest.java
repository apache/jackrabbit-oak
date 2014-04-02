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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.collect.Lists.newArrayList;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.junit.Before;
import org.junit.Test;

public class FileStoreTest {

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile(
                "FileStoreTest", "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @Test
    public void testRecovery() throws IOException {
        FileStore store = new FileStore(directory, 1, false);
        store.flush(); // first 1kB

        SegmentNodeState base = store.getHead();
        SegmentNodeBuilder builder = base.builder();
        builder.setProperty("step", "a");
        store.setHead(base, builder.getNodeState());
        store.flush(); // second 1kB

        base = store.getHead();
        builder = base.builder();
        builder.setProperty("step", "b");
        store.setHead(base, builder.getNodeState());
        store.close(); // third 1kB

        store = new FileStore(directory, 1, false);
        assertEquals("b", store.getHead().getString("step"));
        store.close();

        RandomAccessFile file = new RandomAccessFile(
                new File(directory, "data00000a.tar"), "rw");
        file.setLength(2048);
        file.close();

        store = new FileStore(directory, 1, false);
        assertEquals("a", store.getHead().getString("step"));
        store.close();

        file = new RandomAccessFile(
                new File(directory, "data00000a.tar"), "rw");
        file.setLength(1024);
        file.close();

        store = new FileStore(directory, 1, false);
        assertFalse(store.getHead().hasProperty("step"));
        store.close();
    }

    @Test
    public void testRearrangeOldData() throws IOException {
        new FileOutputStream(new File(directory, "data00000.tar")).close();
        new FileOutputStream(new File(directory, "data00010a.tar")).close();
        new FileOutputStream(new File(directory, "data00030.tar")).close();
        new FileOutputStream(new File(directory, "bulk00002.tar")).close();
        new FileOutputStream(new File(directory, "bulk00005a.tar")).close();

        Map<Integer, File> files = FileStore.collectFiles(directory);
        assertEquals(newArrayList(0, 1, 31, 32, 33), newArrayList(files.keySet()));

        assertTrue(new File(directory, "data00000a.tar").isFile());
        assertTrue(new File(directory, "data00001a.tar").isFile());
        assertTrue(new File(directory, "data00031a.tar").isFile());
        assertTrue(new File(directory, "data00032a.tar").isFile());
        assertTrue(new File(directory, "data00033a.tar").isFile());

        files = FileStore.collectFiles(directory);
        assertEquals(newArrayList(0, 1, 31, 32, 33), newArrayList(files.keySet()));
    }

}
