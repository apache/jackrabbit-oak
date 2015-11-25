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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static junitx.framework.ComparableAssert.assertEquals;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentParser.BlobType.LONG;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentParser.BlobType.MEDIUM;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentParser.BlobType.SMALL;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_10;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.apache.jackrabbit.oak.plugins.segment.TestUtils.newRecordId;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentParser.BlobInfo;
import org.apache.jackrabbit.oak.plugins.segment.SegmentParser.ListInfo;
import org.apache.jackrabbit.oak.plugins.segment.SegmentParser.MapInfo;
import org.apache.jackrabbit.oak.plugins.segment.SegmentParser.NodeInfo;
import org.apache.jackrabbit.oak.plugins.segment.SegmentParser.ValueInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SegmentParserTest {
    private final SegmentVersion segmentVersion;

    private SegmentStore store;
    private SegmentWriter writer;

    @Parameterized.Parameters
    public static List<SegmentVersion[]> fixtures() {
        return ImmutableList.of(new SegmentVersion[] {V_10}, new SegmentVersion[] {V_11});
    }

    public SegmentParserTest(SegmentVersion segmentVersion) {
        this.segmentVersion = segmentVersion;
    }

    private static class TestParser extends SegmentParser {
        private final String name;

        private TestParser(String name) {
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
    public void setup() {
        store = mock(SegmentStore.class, withSettings().stubOnly());
        SegmentTracker tracker = new SegmentTracker(store);
        when(store.getTracker()).thenReturn(tracker);
        writer = new SegmentWriter(store, segmentVersion);
    }

    @Test
    public void emptyNode() {
        SegmentNodeState node = writer.writeNode(EMPTY_NODE);
        NodeInfo info = new TestParser("emptyNode") {
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
        }.parseNode(node.getRecordId());
        assertEquals(node.getRecordId(), info.nodeId);
        assertEquals(0, info.nodeCount);
        assertEquals(0, info.propertyCount);
        assertEquals(3, info.size);
    }

    @Test
    public void singleChildNode() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child");
        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        NodeInfo info = new TestParser("singleChildNode") {
            @Override protected void onNode(RecordId parentId, RecordId nodeId) { }
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
        }.parseNode(node.getRecordId());
        assertEquals(node.getRecordId(), info.nodeId);
        assertEquals(1, info.nodeCount);
        assertEquals(0, info.propertyCount);
        assertEquals(6, info.size);
    }

    @Test
    public void node() {
        final NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("one");
        builder.setChildNode("two");
        builder.setProperty("three", 42);
        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        NodeInfo info = new TestParser("node") {
            @Override protected void onNode(RecordId parentId, RecordId nodeId) { }
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
            @Override protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) { }
            @Override protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) { }
            @Override
            protected void onList(RecordId parentId, RecordId listId, int count) {
                if (segmentVersion == V_10) {
                    super.onList(parentId, listId, count);
                }
            }
        }.parseNode(node.getRecordId());
        assertEquals(node.getRecordId(), info.nodeId);
        assertEquals(2, info.nodeCount);
        assertEquals(1, info.propertyCount);
        assertEquals(9, info.size);
    }

    @Test
    public void template() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("n");
        builder.setProperty("p", 1);
        builder.setProperty("jcr:primaryType", "type", NAME);
        builder.setProperty("jcr:mixinTypes", ImmutableList.of("type1", "type2"), NAMES);
        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        NodeInfo nodeInfo = new TestParser("template") {
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
                assertEquals(20, info.size);
            }
            @Override protected void onString(RecordId parentId, RecordId stringId) { }
            @Override protected void onNode(RecordId parentId, RecordId nodeId) { }
            @Override protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) { }
            @Override
            protected void onList(RecordId parentId, RecordId listId, int count) {
                if (segmentVersion == V_10) {
                    super.onList(parentId, listId, count);
                }
            }
        }.parseNode(node.getRecordId());
    }

    @Test
    public void emptyMap() {
        Map<String, RecordId> empty = newHashMap();
        MapRecord map = writer.writeMap(null, empty);
        MapInfo mapInfo = new TestParser("emptyMap") {
            @Override protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) { }
        }.parseMap(null, map.getRecordId(), map);
        assertEquals(map.getRecordId(), mapInfo.mapId);
        assertEquals(-1, mapInfo.size);
    }

    @Test
    public void nonEmptyMap() {
        Random rnd = new Random();
        MapRecord base = writer.writeMap(null, createMap(33, rnd));
        MapRecord map = writer.writeMap(base, createMap(1, rnd));
        final AtomicInteger size = new AtomicInteger();
        MapInfo mapInfo = new TestParser("nonEmptyMap") {
            @Override
            protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
                MapInfo mapInfo = parseMapDiff(mapId, map);
                assertEquals(mapId, mapInfo.mapId);
                size.addAndGet(mapInfo.size);
            }
            @Override
            protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) {
                MapInfo mapInfo = parseMap(parentId, mapId, map);
                assertEquals(mapId, mapInfo.mapId);
                size.addAndGet(mapInfo.size);
            }
            @Override
            protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
                MapInfo mapInfo = parseMapBranch(mapId, map);
                assertEquals(mapId, mapInfo.mapId);
                size.addAndGet(mapInfo.size);
            }
            @Override
            protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
                MapInfo mapInfo = parseMapLeaf(mapId, map);
                assertEquals(mapId, mapInfo.mapId);
                size.addAndGet(mapInfo.size);
            }
            @Override protected void onString(RecordId parentId, RecordId stringId) { }
        }.parseMap(null, map.getRecordId(), map);
        assertEquals(map.getRecordId(), mapInfo.mapId);
        assertEquals(-1, mapInfo.size);
        assertEquals(456, size.get());
    }

    private Map<String, RecordId> createMap(int size, Random rnd) {
        Map<String, RecordId> map = newHashMap();
        for (int k = 0; k < size; k++) {
            map.put("k" + k, newRecordId(store.getTracker(), rnd));
        }
        return map;
    }

    @Test
    public void singleValueProperty() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("p", 1);
        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        NodeInfo nodeInfo = new TestParser("singleValueProperty") {
            @Override
            protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
                PropertyInfo propertyInfo = parseProperty(parentId, propertyId, template);
                assertEquals(propertyId, propertyInfo.propertyId);
                assertEquals(-1, propertyInfo.count);
                assertEquals(0, propertyInfo.size);
            }
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
            @Override protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) { }
            @Override
            protected void onList(RecordId parentId, RecordId listId, int count) {
                if (segmentVersion == V_10) {
                    super.onList(parentId, listId, count);
                }
            }
        }.parseNode(node.getRecordId());
    }

    @Test
    public void multiValueProperty() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("p", ImmutableList.of(1L, 2L, 3L, 4L), LONGS);
        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        NodeInfo nodeInfo = new TestParser("multiValueProperty") {
            @Override
            protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
                PropertyInfo propertyInfo = parseProperty(parentId, propertyId, template);
                assertEquals(propertyId, propertyInfo.propertyId);
                assertEquals(4, propertyInfo.count);
                assertEquals(7, propertyInfo.size);
            }
            @Override protected void onTemplate(RecordId parentId, RecordId templateId) { }
            @Override protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) { }
            @Override protected void onList(RecordId parentId, RecordId listId, int count) { }
        }.parseNode(node.getRecordId());
    }

    @Test
    public void smallBlob() throws IOException {
        SegmentBlob blob = writer.writeBlob(createRandomBlob(4));
        ValueInfo valueInfo = new TestParser("smallBlob") {
            @Override
            protected void onBlob(RecordId parentId, RecordId blobId) {
                BlobInfo blobInfo = parseBlob(blobId);
                assertEquals(blobId, blobInfo.blobId);
                assertEquals(SMALL, blobInfo.blobType);
                assertEquals(5, blobInfo.size);
            }
        }.parseValue(null, blob.getRecordId(), BINARY);
        assertEquals(blob.getRecordId(), valueInfo.valueId);
        assertEquals(BINARY, valueInfo.type);
    }

    @Test
    public void mediumBlob() throws IOException {
        SegmentBlob blob = writer.writeBlob(createRandomBlob(SMALL_LIMIT));
        ValueInfo valueInfo = new TestParser("mediumBlob") {
            @Override
            protected void onBlob(RecordId parentId, RecordId blobId) {
                BlobInfo blobInfo = parseBlob(blobId);
                assertEquals(blobId, blobInfo.blobId);
                assertEquals(MEDIUM, blobInfo.blobType);
                assertEquals(SMALL_LIMIT + 2, blobInfo.size);
            }
        }.parseValue(null, blob.getRecordId(), BINARY);
        assertEquals(blob.getRecordId(), valueInfo.valueId);
        assertEquals(BINARY, valueInfo.type);
    }

    @Test
    public void longBlob() throws IOException {
        SegmentBlob blob = writer.writeBlob(createRandomBlob(MEDIUM_LIMIT));
        ValueInfo valueInfo = new TestParser("longBlob") {
            @Override
            protected void onBlob(RecordId parentId, RecordId blobId) {
                BlobInfo blobInfo = parseBlob(blobId);
                assertEquals(blobId, blobInfo.blobId);
                assertEquals(LONG, blobInfo.blobType);
                assertEquals(MEDIUM_LIMIT + 11, blobInfo.size);
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
    public void shortString() {
        RecordId stringId = writer.writeString("short");
        BlobInfo blobInfo = new TestParser("shortString").parseString(stringId);
        assertEquals(stringId, blobInfo.blobId);
        assertEquals(SMALL, blobInfo.blobType);
        assertEquals(6, blobInfo.size);
    }

    @Test
    public void mediumString() {
        RecordId stringId = writer.writeString(repeat("s", SMALL_LIMIT));
        BlobInfo blobInfo = new TestParser("mediumString").parseString(stringId);
        assertEquals(stringId, blobInfo.blobId);
        assertEquals(MEDIUM, blobInfo.blobType);
        assertEquals(SMALL_LIMIT + 2, blobInfo.size);
    }

    @Test
    public void longString() {
        RecordId stringId = writer.writeString(repeat("s", MEDIUM_LIMIT));
        BlobInfo blobInfo = new TestParser("longString"){
            @Override protected void onList(RecordId parentId, RecordId listId, int count) { }
        }.parseString(stringId);
        assertEquals(stringId, blobInfo.blobId);
        assertEquals(LONG, blobInfo.blobType);
        assertEquals(MEDIUM_LIMIT + 11, blobInfo.size);
    }

    @Test
    public void emptyList() {
        RecordId listId = newRecordId(store.getTracker(), new Random());
        ListInfo listInfo = new TestParser("emptyList").parseList(null, listId, 0);
        assertEquals(listId, listInfo.listId);
        assertEquals(0, listInfo.count);
        assertEquals(0, listInfo.size);
    }

    @Test
    public void nonEmptyList() {
        int count = 100000;
        Random rnd = new Random();
        List<RecordId> list = newArrayListWithCapacity(count);
        for (int k = 0; k < count; k++) {
            list.add(newRecordId(store.getTracker(), rnd));
        }
        RecordId listId = writer.writeList(list);
        ListInfo listInfo = new TestParser("nonEmptyList"){
            @Override
            protected void onListBucket(RecordId parentId, RecordId listId, int index, int count, int capacity) {
                parseListBucket(listId, index, count, capacity);
            }
        }.parseList(null, listId, count);
        assertEquals(listId, listInfo.listId);
        assertEquals(count, listInfo.count);
        assertEquals(301185, listInfo.size);
    }

}
