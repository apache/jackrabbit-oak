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
package org.apache.jackrabbit.oak.index.indexer.document;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TopKSlowestPathsTest {

    @Test
    public void testSlowestTopKElements() {
        TopKSlowestPaths topK = new TopKSlowestPaths(3);
        topK.add("path4", 400);
        topK.add("path2", 200);
        topK.add("path1", 100);
        topK.add("path3", 300);
        topK.add("path5", 500);
        TopKSlowestPaths.PathAndTime[] entries = topK.getTopK();
        System.out.println(Arrays.toString(entries));
        assertEquals(3, entries.length);
        assertEquals("path5", entries[0].path);
        assertEquals(500, entries[0].timeMillis);
        assertEquals("path4", entries[1].path);
        assertEquals(400, entries[1].timeMillis);
        assertEquals("path3", entries[2].path);
        assertEquals(300, entries[2].timeMillis);
    }

    @Test
    public void testToString() {
        TopKSlowestPaths topK = new TopKSlowestPaths(3);
        topK.add("path4", 400);
        topK.add("path2", 200);
        topK.add("path1", 100);
        topK.add("path3", 300);
        topK.add("path5", 500);
        assertEquals("[path5: 500; path4: 400; path3: 300]", topK.toString());
    }

}