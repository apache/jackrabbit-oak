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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

public class MongoParallelDownloadCoordinatorTest {

    static class NodeDocumentArrayBuilder {

        private final ArrayList<NodeDocument> list = new ArrayList<>();
        private final MemoryDocumentStore docStore = new MemoryDocumentStore();

        public static NodeDocumentArrayBuilder create() {
            return new NodeDocumentArrayBuilder();
        }

        public NodeDocumentArrayBuilder add(long modified, String id) {
            var d = new NodeDocument(docStore, 0);
            d.put(NodeDocument.ID, id);
            d.put(NodeDocument.MODIFIED_IN_SECS, modified);
            list.add(d);
            return this;
        }

        public NodeDocument[] build() {
            return list.toArray(new NodeDocument[0]);
        }
    }


    @Test
    public void noIntersection() {
        var m = new MongoParallelDownloadCoordinator();

        NodeDocument[] ascendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                     .add(1L, "a")
                                                                     .add(2L, "b")
                                                                     .add(3L, "c")
                                                                     .build();

        NodeDocument[] descendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                      .add(10L, "z")
                                                                      .add(9L, "y")
                                                                      .add(8L, "x")
                                                                      .add(7L, "w")
                                                                      .build();

        assertEquals(3, m.extendLowerRange(ascendingOrderBatch, ascendingOrderBatch.length));
        assertEquals(4, m.extendUpperRange(descendingOrderBatch, descendingOrderBatch.length));

        assertDownloadPositionEquals(3L, "c", m.getLowerRangeTop());
        assertDownloadPositionEquals(7L, "w", m.getUpperRangeBottom());
    }

    @Test
    public void noIntersectionModifiedEqualsIdHigher() {
        var m = new MongoParallelDownloadCoordinator();

        NodeDocument[] ascendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                     .add(1L, "a")
                                                                     .add(2L, "b")
                                                                     .add(3L, "c")
                                                                     .build();

        NodeDocument[] descendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                      .add(4L, "e")
                                                                      .add(3L, "d")
                                                                      .build();

        assertEquals(3, m.extendLowerRange(ascendingOrderBatch, ascendingOrderBatch.length));
        assertEquals(2, m.extendUpperRange(descendingOrderBatch, descendingOrderBatch.length));

        assertDownloadPositionEquals(3L, "c", m.getLowerRangeTop());
        assertDownloadPositionEquals(3L, "d", m.getUpperRangeBottom());
    }

    @Test
    public void descendingBatchLastElementEqualsBottomRangeTop() {
        var m = new MongoParallelDownloadCoordinator();
        NodeDocument[] ascendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                     .add(1L, "a")
                                                                     .add(2L, "b")
                                                                     .add(3L, "c")
                                                                     .build();
        NodeDocument[] descendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                      .add(4L, "d")
                                                                      .add(3L, "c")
                                                                      .build();

        assertEquals(3, m.extendLowerRange(ascendingOrderBatch, ascendingOrderBatch.length));
        assertEquals(1, m.extendUpperRange(descendingOrderBatch, descendingOrderBatch.length));

        assertDownloadPositionEquals(3L, "c", m.getLowerRangeTop());
        assertDownloadPositionEquals(4L, "d", m.getUpperRangeBottom());
    }

    @Test
    public void intersectionPartial() {
        var m = new MongoParallelDownloadCoordinator();
        NodeDocument[] ascendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                     .add(1L, "a")
                                                                     .add(2L, "b")
                                                                     .add(3L, "c")
                                                                     .build();

        NodeDocument[] descendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                      .add(4L, "z")
                                                                      .add(3L, "y")
                                                                      .add(2L, "x")
                                                                      .build();

        // Should add all elements
        assertEquals(3, m.extendLowerRange(ascendingOrderBatch, ascendingOrderBatch.length));
        // Only add the first two elements because descendingOrderBatch[2] is already in the range
        assertEquals(2, m.extendUpperRange(descendingOrderBatch, descendingOrderBatch.length));

        assertDownloadPositionEquals(3L, "c", m.getLowerRangeTop());
        assertDownloadPositionEquals(3L, "y", m.getUpperRangeBottom());
    }

    @Test
    public void intersectionFull() {
        var m = new MongoParallelDownloadCoordinator();
        NodeDocument[] ascendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                     .add(1L, "a")
                                                                     .add(2L, "b")
                                                                     .add(3L, "c")
                                                                     .build();

        NodeDocument[] descendingOrderBatch = NodeDocumentArrayBuilder.create()
                                                                      .add(2L, "z")
                                                                      .add(1L, "x")
                                                                      .build();

        // Should add all elements
        assertEquals(3, m.extendLowerRange(ascendingOrderBatch, ascendingOrderBatch.length));
        // Do not add any element.
        assertEquals(0, m.extendUpperRange(descendingOrderBatch, descendingOrderBatch.length));

        assertDownloadPositionEquals(3L, "c", m.getLowerRangeTop());
        assertDownloadPositionEquals(Long.MAX_VALUE, null, m.getUpperRangeBottom());
    }

    private void assertDownloadPositionEquals(long expectedModified, String expectedId,
        MongoParallelDownloadCoordinator.DownloadPosition actualPosition) {
        assertEquals(expectedId, actualPosition.lastId);
        assertEquals(expectedModified, actualPosition.lastModified);
    }
}
