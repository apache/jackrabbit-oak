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

import static java.lang.Math.min;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordTest {

    private static final Logger LOG = LoggerFactory.getLogger(RecordTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore store;

    private SegmentWriter writer;

    private final Random random = new Random(0xcafefaceL);

    @Before
    public void setup() throws Exception {
        store = fileStoreBuilder(folder.getRoot()).build();
        writer = store.getWriter();
    }

    @After
    public void tearDown() {
        store.close();
    }

    @Test
    public void testStreamRecord() throws IOException {
        checkRandomStreamRecord(0);
        checkRandomStreamRecord(1);
        checkRandomStreamRecord(0x79);
        checkRandomStreamRecord(0x80);
        checkRandomStreamRecord(0x4079);
        checkRandomStreamRecord(0x4080);
        checkRandomStreamRecord(SegmentStream.BLOCK_SIZE);
        checkRandomStreamRecord(SegmentStream.BLOCK_SIZE + 1);
        checkRandomStreamRecord(Segment.MAX_SEGMENT_SIZE);
        checkRandomStreamRecord(Segment.MAX_SEGMENT_SIZE + 1);
        checkRandomStreamRecord(Segment.MAX_SEGMENT_SIZE * 2);
        checkRandomStreamRecord(Segment.MAX_SEGMENT_SIZE * 2 + 1);
    }

    private void checkRandomStreamRecord(int size) throws IOException {
        byte[] source = new byte[size];
        random.nextBytes(source);

        Blob value = new SegmentBlob(store.getBlobStore(), writer.writeStream(new ByteArrayInputStream(source)));
        InputStream stream = value.getNewStream();
        checkBlob(source, value, 0);
        checkBlob(source, value, 1);
        checkBlob(source, value, 42);
        checkBlob(source, value, 16387);
        checkBlob(source, value, Integer.MAX_VALUE);
    }

    private static void checkBlob(byte[] expected, Blob actual, int skip) throws IOException {
        try (InputStream stream = actual.getNewStream()) {
            stream.skip(skip);
            byte[] b = new byte[349]; // prime number
            int offset = min(skip, expected.length);
            for (int n = stream.read(b); n != -1; n = stream.read(b)) {
                for (int i = 0; i < n; i++) {
                    assertEquals(expected[offset + i], b[i]);
                }
                offset += n;
            }
            assertEquals(offset, expected.length);
            assertEquals(-1, stream.read());
        }
    }

    @Test
    public void testEmptyNode() throws IOException {
        NodeState before = EMPTY_NODE;
        NodeState after = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(before));
        assertEquals(before, after);
    }

    @Test
    public void testSimpleNode() throws IOException {
        NodeState before = EMPTY_NODE.builder()
                .setProperty("foo", "abc")
                .setProperty("bar", 123)
                .setProperty("baz", Math.PI)
                .getNodeState();
        NodeState after = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(before));
        assertEquals(before, after);
    }

    @Test
    public void testDeepNode() throws IOException {
        NodeBuilder root = EMPTY_NODE.builder();
        NodeBuilder builder = root;
        for (int i = 0; i < 1000; i++) {
            builder = builder.child("test");
        }
        NodeState before = builder.getNodeState();
        NodeState after = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(before));
        assertEquals(before, after);
    }

    @Test
    public void testManyMapDeletes() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        for (int i = 0; i < 1000; i++) {
            builder.child("test" + i);
        }
        NodeState before = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        assertEquals(builder.getNodeState(), before);

        builder = before.builder();
        for (int i = 0; i < 900; i++) {
            builder.getChildNode("test" + i).remove();
        }
        NodeState after = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        assertEquals(builder.getNodeState(), after);
    }

    @Test
    public void testMultiValuedBinaryPropertyAcrossSegments()
            throws IOException {
        // biggest possible inlined value record
        byte[] data = new byte[Segment.MEDIUM_LIMIT - 1];
        random.nextBytes(data);

        // create enough copies of the value to fill a full segment
        List<Blob> blobs = new ArrayList<>();
        while (blobs.size() * data.length < Segment.MAX_SEGMENT_SIZE) {
            blobs.add(new SegmentBlob(store.getBlobStore(), writer.writeStream(new ByteArrayInputStream(data))));
        }

        // write a simple node that'll now be stored in a separate segment
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("test", blobs, BINARIES);
        NodeState state = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));

        // all the blobs should still be accessible, even if they're
        // referenced from another segment
        for (Blob blob : state.getProperty("test").getValue(BINARIES)) {
            try {
                blob.getNewStream().close();
            } catch (IllegalStateException e) {
                fail("OAK-1374");
            }
        }
    }

    @Test
    public void testBinaryPropertyFromExternalSegmentStore() throws IOException, CommitFailedException {
        byte[] data = new byte[Segment.MEDIUM_LIMIT + 1];
        random.nextBytes(data);

        SegmentNodeStore extStore = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        NodeBuilder extRootBuilder = extStore.getRoot().builder();
        Blob extBlob = extRootBuilder.createBlob(new ByteArrayInputStream(data));
        extRootBuilder.setProperty("binary", extBlob, BINARY);
        extStore.merge(extRootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        PropertyState extPropertyState = extStore.getRoot().getProperty("binary");

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(extPropertyState);
        NodeState state = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));

        try {
            InputStream is = state.getProperty("binary").getValue(BINARY).getNewStream();
            is.read();
            is.close();
        } catch (SegmentNotFoundException e) {
            fail("OAK-4307 SegmentWriter saves references to external blobs");
        }
    }

    @Test
    public void testStringPrimaryType() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:primaryType", "foo", STRING);
        NodeState state = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        assertNotNull(state.getProperty("jcr:primaryType"));
    }

    @Test
    public void testStringMixinTypes() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:mixinTypes", singletonList("foo"), STRINGS);
        NodeState state = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        assertNotNull(state.getProperty("jcr:mixinTypes"));
    }

    @Test
    public void testReadPropertyPerformance() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:mixinTypes", singletonList("foo"), STRINGS);
        NodeState state = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        assertNotNull(state.getProperty("jcr:mixinTypes"));

        // warmup
        for (int i = 0; i < 1_000_000; i++) {
            state.getProperties().forEach(PropertyState::getName);
        }

        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < 10_000_000; i++) {
            state.getProperties().forEach(PropertyState::getName);
        }
        LOG.info("Read properties in: {}", sw.stop());
    }

}
