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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.core.AbstractOakTest;
import org.junit.Ignore;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

public class LargeKernelNodeStateTest extends AbstractOakTest {

    private final int N = KernelNodeState.MAX_CHILD_NODE_NAMES;

    @Override
    protected NodeState createInitialState() {
        StringBuilder jsop = new StringBuilder("+\"test\":{\"a\":1");
        for (int i = 0; i <= N; i++) {
            jsop.append(",\"x" + i + "\":{}");
        }
        jsop.append('}');
        String revision = microKernel.commit(
                "/", jsop.toString(), microKernel.getHeadRevision(), "test data");
        return new KernelNodeState(microKernel, valueFactory, "/test", revision);
    }

    @Test
    public void testGetChildNodeCount() {
        assertEquals(N + 1, state.getChildNodeCount());
    }

    @Test
    public void testGetChildNode() {
        assertNotNull(state.getChildNode("x0"));
        assertNotNull(state.getChildNode("x1"));
        assertNotNull(state.getChildNode("x" + N));
        assertNull(state.getChildNode("x" + (N + 1)));
    }

    @Test
    @SuppressWarnings("unused")
    public void testGetChildNodeEntries() {
        long count = 0;
        for (ChildNodeEntry entry : state.getChildNodeEntries(0, -1)) {
            count++;
        }
        assertEquals(N + 1, count);
    }

    @Ignore // TODO
    @Test
    @SuppressWarnings("unused")
    public void testGetChildNodeEntriesWithOffset() {
        long count = 0;
        for (ChildNodeEntry entry : state.getChildNodeEntries(N, -1)) {
            count++;
        }
        assertEquals(1, count);

        // Offset beyond the range
        assertFalse(state.getChildNodeEntries(N + 1, -1).iterator().hasNext());
    }

    @Test
    @SuppressWarnings("unused")
    public void testGetChildNodeEntriesWithCount() {
        long count = 0;
        for (ChildNodeEntry entry : state.getChildNodeEntries(0, N + 1)) {
            count++;
        }
        assertEquals(N + 1, count);

        // Zero count
        assertFalse(state.getChildNodeEntries(0, 0).iterator().hasNext());
    }

}
