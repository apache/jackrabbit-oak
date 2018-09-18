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
package org.apache.jackrabbit.oak.spi.xml;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReferenceChangeTrackerTest {

    private ReferenceChangeTracker rct = new ReferenceChangeTracker();

    @Test
    public void testGet() {
        assertNull(rct.get("old"));

        rct.put("old", "new");
        assertEquals("new", rct.get("old"));
    }

    @Test
    public void testClear() {
        rct.put("old", "new");
        rct.clear();

        assertNull(rct.get("old"));
    }

    @Test
    public void testReferenceProcessing() {
        rct.processedReference("ref");
        rct.processedReference("ref2");

        assertTrue(Iterators.elementsEqual(Lists.newArrayList("ref", "ref2").iterator(), rct.getProcessedReferences()));

        rct.removeReferences(ImmutableList.of("ref"));

        assertTrue(Iterators.elementsEqual(Lists.newArrayList("ref2").iterator(), rct.getProcessedReferences()));
    }

}