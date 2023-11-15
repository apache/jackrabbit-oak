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

package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Test;

import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GCIncrementTest {

    @Test
    public void testIsCompactedSameGeneration() {
        GCGeneration base = GCGeneration.NULL;
        GCIncrement increment = new GCIncrement(base, base, base);

        assertFalse(increment.isFullyCompacted(base));
        assertFalse(increment.isFullyCompacted(newGCGeneration(1, 1, true)));
    }

    @Test
    public void testIsCompactedTail() {
        GCGeneration base = GCGeneration.NULL;
        GCIncrement increment = new GCIncrement(base, base.nextPartial(), base.nextTail());

        assertFalse(increment.isFullyCompacted(newGCGeneration(1, 0, false)));
        assertFalse(increment.isFullyCompacted(base));
        assertFalse(increment.isFullyCompacted(base.nextPartial()));
        assertTrue(increment.isFullyCompacted(base.nextTail()));
    }

    @Test
    public void testIsCompactedFull() {
        GCGeneration base = GCGeneration.NULL;
        GCIncrement increment = new GCIncrement(base, base.nextPartial(), base.nextFull());

        assertFalse(increment.isFullyCompacted(base));
        assertFalse(increment.isFullyCompacted(newGCGeneration(1, 1, false)));
        assertFalse(increment.isFullyCompacted(base.nextPartial()));
        assertFalse(increment.isFullyCompacted(base.nextTail()));
        assertTrue(increment.isFullyCompacted(base.nextFull()));
    }

}
