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
package org.apache.jackrabbit.oak.plugins.document;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MemoryDiffCacheKeyTest {

    @Test
    public void asString() {
        Path p = Path.fromString("/foo/bar");
        RevisionVector from = RevisionVector.fromString("r3-0-1");
        RevisionVector to = RevisionVector.fromString("r7-0-1");
        MemoryDiffCache.Key key = new MemoryDiffCache.Key(p, from, to);
        assertEquals("r3-0-1/foo/bar@r7-0-1", key.asString());
    }

    @Test
    public void fromString() {
        Path p = Path.fromString("/foo/bar");
        RevisionVector from = RevisionVector.fromString("r3-0-1");
        RevisionVector to = RevisionVector.fromString("r7-0-1");
        MemoryDiffCache.Key expected = new MemoryDiffCache.Key(p, from, to);

        String s = "r3-0-1/foo/bar@r7-0-1";
        MemoryDiffCache.Key key = MemoryDiffCache.Key.fromString(s);
        assertEquals(expected, key);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromStringIllegalArgumentException() {
        MemoryDiffCache.Key.fromString("foo@r7-0-1");
    }
}
