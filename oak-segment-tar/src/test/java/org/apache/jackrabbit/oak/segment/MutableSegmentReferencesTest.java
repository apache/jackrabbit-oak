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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Test;

public class MutableSegmentReferencesTest {

    @Test
    public void referencesShouldBeGreaterThanZero() throws Exception {
        MemoryStore store = new MemoryStore();
        SegmentId id = store.getSegmentIdProvider().newDataSegmentId();

        MutableSegmentReferences table = new MutableSegmentReferences();
        int reference = table.addOrReference(id);
        assertEquals(1, reference);
    }

    @Test
    public void referencesShouldBeIncrementing() throws Exception {
        MemoryStore store = new MemoryStore();
        SegmentId first = store.getSegmentIdProvider().newDataSegmentId();
        SegmentId second = store.getSegmentIdProvider().newDataSegmentId();

        MutableSegmentReferences table = new MutableSegmentReferences();
        int firstReference = table.addOrReference(first);
        int secondReference = table.addOrReference(second);
        assertEquals(firstReference + 1, secondReference);
    }

    @Test
    public void shouldAddNewSegmentReference() throws Exception {
        MemoryStore store = new MemoryStore();
        SegmentId id = store.getSegmentIdProvider().newDataSegmentId();

        MutableSegmentReferences table = new MutableSegmentReferences();
        int reference = table.addOrReference(id);
        assertEquals(id, table.getSegmentId(reference));
    }

    @Test
    public void shouldNotAddSameSegmentIdTwice() throws Exception {
        MemoryStore store = new MemoryStore();
        SegmentId id = store.getSegmentIdProvider().newDataSegmentId();

        MutableSegmentReferences table = new MutableSegmentReferences();
        int first = table.addOrReference(id);
        int second = table.addOrReference(id);
        assertEquals(first, second);
    }

    @Test
    public void shouldMaintainSize() throws Exception {
        MemoryStore store = new MemoryStore();
        SegmentId id = store.getSegmentIdProvider().newDataSegmentId();

        MutableSegmentReferences table = new MutableSegmentReferences();
        assertEquals(0, table.size());
        table.addOrReference(id);
        assertEquals(1, table.size());
    }

    @Test
    public void shouldContainAddedSegment() throws Exception {
        MemoryStore store = new MemoryStore();
        SegmentId id = store.getSegmentIdProvider().newDataSegmentId();

        MutableSegmentReferences table = new MutableSegmentReferences();
        assertFalse(table.contains(id));
        table.addOrReference(id);
        assertTrue(table.contains(id));
    }

    @Test
    public void shouldIterateInInsertionOrder() throws Exception {
        MemoryStore store = new MemoryStore();
        SegmentId first = store.getSegmentIdProvider().newDataSegmentId();
        SegmentId second = store.getSegmentIdProvider().newDataSegmentId();
        List<SegmentId> ids = newArrayList(first, second);

        MutableSegmentReferences table = new MutableSegmentReferences();
        table.addOrReference(first);
        table.addOrReference(second);
        assertTrue(elementsEqual(ids, table));
    }

}
