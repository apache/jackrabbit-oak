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

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class KernelNodeBuilderTest {

    @Test
    public void deletesKernelNodeStore() throws CommitFailedException {
        NodeStore store = new KernelNodeStore(new MicroKernelImpl());
        init(store);
        run(store);
    }

    @Test
    public void deletesMemoryNodeStore() throws CommitFailedException {
        NodeStore store = new MemoryNodeStore();
        init(store);
        run(store);
    }

    private static void init(NodeStore store) throws CommitFailedException {
        NodeStoreBranch branch = store.branch();
        NodeBuilder builder = branch.getRoot().builder();
        builder.child("x").child("y").child("z");
        branch.setRoot(builder.getNodeState());
        branch.merge();
    }

    private static void run(NodeStore store) throws CommitFailedException {
        NodeStoreBranch branch = store.branch();
        NodeBuilder builder = branch.getRoot().builder();

        assertTrue("child node x should be present", builder.hasChildNode("x"));
        assertTrue("child node x/y should be present", builder.child("x")
                .hasChildNode("y"));
        assertTrue("child node x/y/z should be present", builder.child("x")
                .child("y").hasChildNode("z"));

        builder.removeNode("x");
        assertFalse("child node x not should be present",
                builder.hasChildNode("x"));
        assertFalse("child node x/y not should be present", builder.child("x")
                .hasChildNode("y"));

        // See OAK-531
        assertFalse("child node x/y/z not should not be present", builder
                .child("x").child("y").hasChildNode("z"));

        branch.merge();
    }

}
