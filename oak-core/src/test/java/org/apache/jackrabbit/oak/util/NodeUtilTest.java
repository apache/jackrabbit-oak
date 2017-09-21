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
package org.apache.jackrabbit.oak.util;

import java.util.Map;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class NodeUtilTest extends AbstractSecurityTest {

    private NodeUtil node;

    @Override
    public void before() throws Exception {
        super.before();

        node = new NodeUtil(root.getTree("/"));
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    private void assertEqualNodeUtil(@Nonnull NodeUtil expected, @Nonnull NodeUtil result) {
        assertEquals(expected.getTree().getPath(), result.getTree().getPath());
    }

    @Test
    public void testGetOrAddTree() throws Exception {
        NodeUtil result = node.getOrAddTree(".", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertSame(node, result);

        NodeUtil child = node.getOrAddTree("child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertEqualNodeUtil(new NodeUtil(root.getTree("/child")), child);

        NodeUtil parent = child.getOrAddTree("..", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertEqualNodeUtil(node, parent);

        Map<String, String> map = ImmutableMap.of(
                "a/b/c", "/a/b/c",
                "a/../b/c", "/b/c",
                "a/b/c/../..", "/a",
                "a/././././b/c", "/a/b/c"
        );
        for (String relPath : map.keySet()) {
            NodeUtil n = node.getOrAddTree(relPath, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            assertEqualNodeUtil(new NodeUtil(root.getTree(map.get(relPath))), n);
        }
    }
}