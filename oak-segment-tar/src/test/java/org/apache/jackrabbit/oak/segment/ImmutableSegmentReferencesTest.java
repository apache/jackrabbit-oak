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

public class ImmutableSegmentReferencesTest {

    @Test
    public void shouldNotBeIndirectlyModifiable() throws Exception {
        SegmentStore store = new MemoryStore();
        List<SegmentId> list = newArrayList(store.newDataSegmentId());
        ImmutableSegmentReferences table = new ImmutableSegmentReferences(list);
        list.add(store.newDataSegmentId());
        assertFalse(elementsEqual(list, table));
    }

    @Test
    public void shouldMaintainIterationOrder() throws Exception {
        SegmentStore store = new MemoryStore();
        List<SegmentId> list = newArrayList(store.newDataSegmentId());
        ImmutableSegmentReferences table = new ImmutableSegmentReferences(list);
        assertTrue(elementsEqual(list, table));
    }

    @Test
    public void shouldRetrieveSegmentIdByReference() throws Exception {
        SegmentStore store = new MemoryStore();
        SegmentId id = store.newDataSegmentId();
        List<SegmentId> list = newArrayList(id);
        ImmutableSegmentReferences table = new ImmutableSegmentReferences(list);
        assertEquals(id, table.getSegmentId(1));
    }

}
