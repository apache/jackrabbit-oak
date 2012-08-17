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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.core.AbstractCoreTest;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

public class LargeKernelNodeStateTest extends AbstractCoreTest {
    private static final int N = KernelNodeState.MAX_CHILD_NODE_NAMES;

    @Override
    protected NodeState createInitialState(MicroKernel microKernel) {
        StringBuilder jsop = new StringBuilder("+\"test\":{\"a\":1");
        for (int i = 0; i <= N; i++) {
            jsop.append(",\"x").append(i).append("\":{}");
        }
        jsop.append('}');
        microKernel.commit("/", jsop.toString(), null, "test data");
        return store.getRoot().getChildNode("test");
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
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            count++;
        }
        assertEquals(N + 1, count);
    }

}
