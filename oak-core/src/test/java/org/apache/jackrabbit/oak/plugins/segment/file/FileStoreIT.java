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
import static com.google.common.collect.Sets.newTreeSet;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.SEGMENT_MK;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Strings;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.Compactor;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileStoreIT {

    private File directory;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(getFixtures().contains(SEGMENT_MK));
    }

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile(
                "FileStoreIT", "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @After
    public void tearDown() {
        try {
            deleteDirectory(directory);
        } catch (IOException e) {
            //
        }
    }

    @Test
    public void testRestartAndGCWithoutMM() throws IOException {
        testRestartAndGC(false);
    }

    @Test
    public void testRestartAndGCWithMM() throws IOException {
        testRestartAndGC(true);
    }

    public void testRestartAndGC(boolean memoryMapping) throws IOException {
        FileStore store = new FileStore(directory, 1, memoryMapping);
        store.close();

        store = new FileStore(directory, 1, memoryMapping);
        SegmentNodeState base = store.getHead();
        SegmentNodeBuilder builder = base.builder();
        byte[] data = new byte[10 * 1024 * 1024];
        new Random().nextBytes(data);
        Blob blob = builder.createBlob(new ByteArrayInputStream(data));
        builder.setProperty("foo", blob);
        store.setHead(base, builder.getNodeState());
        store.flush();
        store.setHead(store.getHead(), base);
        store.close();

        store = new FileStore(directory, 1, memoryMapping);
        store.gc();
        store.flush();
        store.close();

        store = new FileStore(directory, 1, memoryMapping);
        store.close();
    }

    @Test
    public void testCompaction() throws IOException {
        int largeBinarySize = 10 * 1024 * 1024;

        FileStore store = new FileStore(directory, 1, false);
        SegmentWriter writer = store.getTracker().getWriter();

        SegmentNodeState base = store.getHead();
        SegmentNodeBuilder builder = base.builder();
        byte[] data = new byte[largeBinarySize];
        new Random().nextBytes(data);
        SegmentBlob blob = writer.writeStream(new ByteArrayInputStream(data));
        builder.setProperty("foo", blob);
        builder.getNodeState(); // write the blob reference to the segment
        builder.setProperty("foo", "bar");
        SegmentNodeState head = builder.getNodeState();
        assertTrue(store.setHead(base, head));
        assertEquals("bar", store.getHead().getString("foo"));

        Compactor compactor = new Compactor(store);
        SegmentNodeState compacted =
                compactor.compact(EMPTY_NODE, head, EMPTY_NODE);
        store.close();

        // First simulate the case where during compaction a reference to the
        // older segments is added to a segment that the compactor is writing
        store = new FileStore(directory, 1, false);
        head = store.getHead();
        assertTrue(store.size() > largeBinarySize);
        builder = head.builder();
        builder.setChildNode("old", head); // reference to pre-compacted state
        builder.getNodeState();
        assertTrue(store.setHead(head, compacted));
        store.close();

        // In this case the revision cleanup is unable to reclaim the old data
        store = new FileStore(directory, 1, false);
        assertTrue(store.size() > largeBinarySize);
        store.cleanup();
        assertTrue(store.size() > largeBinarySize);
        store.close();

        // Now we do the same thing, but let the compactor use a different
        // SegmentWriter
        store = new FileStore(directory, 1, false);
        head = store.getHead();
        assertTrue(store.size() > largeBinarySize);
        writer = new SegmentWriter(store, V_11);
        compactor = new Compactor(store);
        compacted = compactor.compact(EMPTY_NODE, head, EMPTY_NODE);
        builder = head.builder();
        builder.setChildNode("old", head); // reference to pre-compacted state
        builder.getNodeState();
        writer.flush();
        assertTrue(store.setHead(head, compacted));
        store.close();

        // Revision cleanup is now able to reclaim the extra space (OAK-1932)
        store = new FileStore(directory, 1, false);
        assertTrue(store.size() > largeBinarySize);
        store.cleanup();
        assertTrue(store.size() < largeBinarySize);
        store.close();
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

        Map<Integer, ?> files = FileStore.collectFiles(directory);
        assertEquals(
                newArrayList(0, 1, 31, 32, 33),
                newArrayList(newTreeSet(files.keySet())));

        assertTrue(new File(directory, "data00000a.tar").isFile());
        assertTrue(new File(directory, "data00001a.tar").isFile());
        assertTrue(new File(directory, "data00031a.tar").isFile());
        assertTrue(new File(directory, "data00032a.tar").isFile());
        assertTrue(new File(directory, "data00033a.tar").isFile());

        files = FileStore.collectFiles(directory);
        assertEquals(
                newArrayList(0, 1, 31, 32, 33),
                newArrayList(newTreeSet(files.keySet())));
    }

    @Test  // See OAK-2049
    public void segmentOverflow() throws IOException {
        for (int n = 1; n < 255; n++) {  // 255 = ListRecord.LEVEL_SIZE
            FileStore store = new FileStore(directory, 1, false);
            SegmentWriter writer = store.getTracker().getWriter();
            // writer.length == 32  (from the root node)

            // adding 15 strings with 16516 bytes each
            for (int k = 0; k < 15; k++) {
                // 16516 = (Segment.MEDIUM_LIMIT - 1 + 2 + 3)
                // 1 byte per char, 2 byte to store the length and 3 bytes for the
                // alignment to the integer boundary
                writer.writeString(Strings.repeat("abcdefghijklmno".substring(k, k + 1),
                        Segment.MEDIUM_LIMIT - 1));
            }

            // adding 14280 bytes. 1 byte per char, and 2 bytes to store the length
            RecordId x = writer.writeString(Strings.repeat("x", 14278));
            // writer.length == 262052

            // Adding 765 bytes (255 recordIds)
            // This should cause the current segment to flush
            List<RecordId> list = Collections.nCopies(n, x);
            writer.writeList(list);

            writer.flush();

            // Don't close the store in a finally clause as if a failure happens
            // this will also fail an cover up the earlier exception
            store.close();
        }
    }

    @Test
    public void nonBlockingROStore() throws IOException {
        FileStore store = new FileStore(directory, 1, false);
        store.flush(); // first 1kB
        SegmentNodeState base = store.getHead();
        SegmentNodeBuilder builder = base.builder();
        builder.setProperty("step", "a");
        store.setHead(base, builder.getNodeState());
        store.flush(); // second 1kB

        ReadOnlyStore ro = null;
        try {
            ro = new ReadOnlyStore(directory);
            assertEquals(store.getHead(), ro.getHead());
        } finally {
            if (ro != null) {
                ro.close();
            }
            store.close();
        }
    }

}
