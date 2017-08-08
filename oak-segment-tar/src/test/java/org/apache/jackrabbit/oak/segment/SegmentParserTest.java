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

import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.segment.SegmentParser.BlobType.LONG;
import static org.apache.jackrabbit.oak.segment.SegmentParser.BlobType.MEDIUM;
import static org.apache.jackrabbit.oak.segment.SegmentParser.BlobType.SMALL;
import static org.apache.jackrabbit.oak.segment.TestUtils.newRecordId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.segment.SegmentParser.BlobInfo;
import org.apache.jackrabbit.oak.segment.SegmentParser.ListInfo;
import org.apache.jackrabbit.oak.segment.SegmentParser.MapInfo;
import org.apache.jackrabbit.oak.segment.SegmentParser.NodeInfo;
import org.apache.jackrabbit.oak.segment.SegmentParser.ValueInfo;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class SegmentParserTest {

    private MemoryStore store;

    private SegmentWriter writer;

    private static class TestParser extends SegmentParser {
        private final String name;

        private TestParser(SegmentReader reader, String name) {
            super(reader);
            this.name = name;
        }

        private void throwUOE(String method) {
            throw new UnsupportedOperationException(name + " must not call " + method);
        }

        @Override
        protected void onNode(RecordId parentId, RecordId nodeId) {
            throwUOE("onNode");
        }

        @Override
        protected void onTemplate(RecordId parentId, RecordId templateId) {
            throwUOE("onTemplate");
        }

        @Override
        protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) {
            throwUOE("onMap");
        }

        @Override
        protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
            throwUOE("onMapDiff");
        }

        @Override
        protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
            throwUOE("onMapLeaf");
        }

        @Override
        protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
            throwUOE("onMapBranch");
        }

        @Override
        protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
            throwUOE("onProperty");
        }

        @Override
        protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) {
            throwUOE("onValue");
        }

        @Override
        protected void onBlob(RecordId parentId, RecordId blobId) {
            throwUOE("onBlob");
        }

        @Override
        protected void onString(RecordId parentId, RecordId stringId) {
            throwUOE("onString");
        }

        @Override
        protected void onList(RecordId parentId, RecordId listId, int count) {
            throwUOE("onList");
        }

        @Override
        protected void onListBucket(RecordId parentId, RecordId listId, int index, int count, int capacity) {
            throwUOE("onListBucket");
        }
    }

    @Before
    public void setup() throws IOException {
        store = new MemoryStore();
        writer = defaultSegmentWriterBuilder("").build(store);
    }

    @Test
    public void emptyNode() throws IOException {
        SegmentNodeState node = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(EMPTY_NODE));
        NodeInfo info = new TestParser(store.getReader(), "emptyNode") {
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
        }.parseNode(node.getRecordId());
        assertEquals(node.getRecordId(), info.nodeId);
        assertEquals(0, info.nodeCount);
        assertEquals(0, info.propertyCount);
        assertEquals(info.nodeId.toString10(), info.stableId);
    }

    @Test
    public void singleChildNode() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child");
        SegmentNodeState node = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        NodeInfo info = new TestParser(store.getReader(), "singleChildNode") {
            @Override protected void onNode(RecordId parentId, RecordId nodeId) { }
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
        }.parseNode(node.getRecordId());
        assertEquals(node.getRecordId(), info.nodeId);
        assertEquals(1, info.nodeCount);
        assertEquals(0, info.propertyCount);
        assertEquals(info.nodeId.toString10(), info.stableId);
    }

    @Test
    public void node() throws IOException {
        final NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("one");
        builder.setChildNode("two");
        builder.setProperty("three", 42);
        SegmentNodeState node = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        NodeInfo info = new TestParser(store.getReader(), "node") {
            @Override protected void onNode(RecordId parentId, RecordId nodeId) { }
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
            @Override protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) { }
            @Override protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) { }
            @Override protected void onList(RecordId parentId, RecordId listId, int count) { }
        }.parseNode(node.getRecordId());
        assertEquals(node.getRecordId(), info.nodeId);
        assertEquals(2, info.nodeCount);
        assertEquals(1, info.propertyCount);
        assertEquals(info.nodeId.toString10(), info.stableId);
    }

    @Test
    public void template() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("n");
        builder.setProperty("p", 1);
        builder.setProperty("jcr:primaryType", "type", NAME);
        builder.setProperty("jcr:mixinTypes", ImmutableList.of("type1", "type2"), NAMES);
        SegmentNodeState node = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        NodeInfo nodeInfo = new TestParser(store.getReader(), "template") {
            @Override
            protected void onTemplate(RecordId parentId, RecordId templateId) {
                TemplateInfo info = parseTemplate(templateId);
                assertEquals(templateId, info.templateId);
                assertTrue(info.hasPrimaryType);
                assertTrue(info.hasMixinType);
                assertFalse(info.zeroChildNodes);
                assertFalse(info.manyChildNodes);
                assertEquals(2, info.mixinCount);
                assertEquals(1, info.propertyCount);
            }
            @Override protected void onString(RecordId parentId, RecordId stringId) { }
            @Override protected void onNode(RecordId parentId, RecordId nodeId) { }
            @Override protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) { }
            @Override protected void onList(RecordId parentId, RecordId listId, int count) { }
        }.parseNode(node.getRecordId());
    }

    @Test
    public void emptyMap() throws IOException {
        Map<String, RecordId> empty = newHashMap();
        MapRecord map = new MapRecord(store.getReader(), writer.writeMap(null, empty));
        MapInfo mapInfo = new TestParser(store.getReader(), "emptyMap") {
            @Override protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) { }
        }.parseMap(null, map.getRecordId(), map);
        assertEquals(map.getRecordId(), mapInfo.mapId);
    }

    @Test
    public void nonEmptyMap() throws IOException {
        Random rnd = new Random();
        MapRecord base = new MapRecord(store.getReader(), writer.writeMap(null, createMap(33, rnd)));
        MapRecord map = new MapRecord(store.getReader(), writer.writeMap(base, createMap(1, rnd)));
        MapInfo mapInfo = new TestParser(store.getReader(), "nonEmptyMap") {
            @Override
            protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
                MapInfo mapInfo = parseMapDiff(mapId, map);
                assertEquals(mapId, mapInfo.mapId);
            }
            @Override
            protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) {
                MapInfo mapInfo = parseMap(parentId, mapId, map);
                assertEquals(mapId, mapInfo.mapId);
            }
            @Override
            protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
                MapInfo mapInfo = parseMapBranch(mapId, map);
                assertEquals(mapId, mapInfo.mapId);
            }
            @Override
            protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
                MapInfo mapInfo = parseMapLeaf(mapId, map);
                assertEquals(mapId, mapInfo.mapId);
            }
            @Override protected void onString(RecordId parentId, RecordId stringId) { }
        }.parseMap(null, map.getRecordId(), map);
        assertEquals(map.getRecordId(), mapInfo.mapId);
    }

    private Map<String, RecordId> createMap(int size, Random rnd) throws IOException {
        Map<String, RecordId> map = newHashMap();
        for (int k = 0; k < size; k++) {
            map.put("k" + k, writer.writeString("string" + rnd.nextLong()));
        }
        return map;
    }

    @Test
    public void singleValueProperty() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("p", 1);
        SegmentNodeState node = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        NodeInfo nodeInfo = new TestParser(store.getReader(), "singleValueProperty") {
            @Override
            protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
                PropertyInfo propertyInfo = parseProperty(parentId, propertyId, template);
                assertEquals(propertyId, propertyInfo.propertyId);
                assertEquals(-1, propertyInfo.count);
            }
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
            @Override protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) { }
            @Override protected void onList(RecordId parentId, RecordId listId, int count) { }
        }.parseNode(node.getRecordId());
    }

    @Test
    public void multiValueProperty() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("p", ImmutableList.of(1L, 2L, 3L, 4L), LONGS);
        SegmentNodeState node = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(builder.getNodeState()));
        NodeInfo nodeInfo = new TestParser(store.getReader(), "multiValueProperty") {
            @Override
            protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
                PropertyInfo propertyInfo = parseProperty(parentId, propertyId, template);
                assertEquals(propertyId, propertyInfo.propertyId);
                assertEquals(4, propertyInfo.count);
            }
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
            @Override protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) { }
            @Override protected void onList(RecordId parentId, RecordId listId, int count) { }
        }.parseNode(node.getRecordId());
    }

    @Test
    public void smallBlob() throws IOException {
        SegmentBlob blob = new SegmentBlob(store.getBlobStore(), writer.writeBlob(createRandomBlob(4)));
        ValueInfo valueInfo = new TestParser(store.getReader(), "smallBlob") {
            @Override
            protected void onBlob(RecordId parentId, RecordId blobId) {
                BlobInfo blobInfo = parseBlob(blobId);
                assertEquals(blobId, blobInfo.blobId);
                assertEquals(SMALL, blobInfo.blobType);
            }
        }.parseValue(null, blob.getRecordId(), BINARY);
        assertEquals(blob.getRecordId(), valueInfo.valueId);
        assertEquals(BINARY, valueInfo.type);
    }

    @Test
    public void mediumBlob() throws IOException {
        SegmentBlob blob = new SegmentBlob(store.getBlobStore(), writer.writeBlob(createRandomBlob(SMALL_LIMIT)));
        ValueInfo valueInfo = new TestParser(store.getReader(), "mediumBlob") {
            @Override
            protected void onBlob(RecordId parentId, RecordId blobId) {
                BlobInfo blobInfo = parseBlob(blobId);
                assertEquals(blobId, blobInfo.blobId);
                assertEquals(MEDIUM, blobInfo.blobType);
            }
        }.parseValue(null, blob.getRecordId(), BINARY);
        assertEquals(blob.getRecordId(), valueInfo.valueId);
        assertEquals(BINARY, valueInfo.type);
    }

    @Test
    public void longBlob() throws IOException {
        SegmentBlob blob = new SegmentBlob(store.getBlobStore(), writer.writeBlob(createRandomBlob(MEDIUM_LIMIT)));
        ValueInfo valueInfo = new TestParser(store.getReader(), "longBlob") {
            @Override
            protected void onBlob(RecordId parentId, RecordId blobId) {
                BlobInfo blobInfo = parseBlob(blobId);
                assertEquals(blobId, blobInfo.blobId);
                assertEquals(LONG, blobInfo.blobType);
            }
            @Override protected void onList(RecordId parentId, RecordId listId, int count) { }
        }.parseValue(null, blob.getRecordId(), BINARY);
        assertEquals(blob.getRecordId(), valueInfo.valueId);
        assertEquals(BINARY, valueInfo.type);
    }

    private static Blob createRandomBlob(int size) {
        byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);
        return new ArrayBasedBlob(bytes);
    }

    @Test
    public void shortString() throws IOException {
        RecordId stringId = writer.writeString("short");
        BlobInfo blobInfo = new TestParser(store.getReader(), "shortString").parseString(stringId);
        assertEquals(stringId, blobInfo.blobId);
        assertEquals(SMALL, blobInfo.blobType);
    }

    @Test
    public void mediumString() throws IOException {
        RecordId stringId = writer.writeString(repeat("s", SMALL_LIMIT));
        BlobInfo blobInfo = new TestParser(store.getReader(), "mediumString").parseString(stringId);
        assertEquals(stringId, blobInfo.blobId);
        assertEquals(MEDIUM, blobInfo.blobType);
    }

    @Test
    public void longString() throws IOException {
        RecordId stringId = writer.writeString(repeat("s", MEDIUM_LIMIT));
        BlobInfo blobInfo = new TestParser(store.getReader(), "longString"){
            @Override protected void onList(RecordId parentId, RecordId listId, int count) { }
        }.parseString(stringId);
        assertEquals(stringId, blobInfo.blobId);
        assertEquals(LONG, blobInfo.blobType);
    }

    @Test
    public void emptyList() {
        RecordId listId = newRecordId(store.getSegmentIdProvider(), new Random());
        ListInfo listInfo = new TestParser(store.getReader(), "emptyList").parseList(null, listId, 0);
        assertEquals(listId, listInfo.listId);
        assertEquals(0, listInfo.count);
    }

    @Test
    public void nonEmptyList() throws IOException {
        int count = 100000;
        Random rnd = new Random();
        List<RecordId> list = newArrayListWithCapacity(count);
        for (int k = 0; k < count; k++) {
            list.add(writer.writeString("string " + rnd.nextLong()));
        }
        RecordId listId = writer.writeList(list);
        ListInfo listInfo = new TestParser(store.getReader(), "nonEmptyList"){
            @Override
            protected void onListBucket(RecordId parentId, RecordId listId, int index, int count, int capacity) {
                parseListBucket(listId, index, count, capacity);
            }
        }.parseList(null, listId, count);
        assertEquals(listId, listInfo.listId);
        assertEquals(count, listInfo.count);
    }

}
