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
package org.apache.jackrabbit.oak.query.index;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

/**
 * Tests the TraversingCursor.
 */
public class TraversingIndexTest {

    @Test
    public void traverse() throws Exception {
        NodeBuilder builder = EMPTY_NODE.builder();
        NodeBuilder parents = builder.child("parents");
        parents.child("p0").setProperty("id", 0);
        parents.child("p1").setProperty("id", 1);
        parents.child("p2").setProperty("id", 2);
        NodeBuilder children = builder.child("children");
        children.child("c1").setProperty("p", "1");
        children.child("c2").setProperty("p", "1");
        children.child("c3").setProperty("p", "2");
        children.child("c4").setProperty("p", "3");
        NodeState root = builder.getNodeState();

        TraversingIndex t = new TraversingIndex();

        FilterImpl f = FilterImpl.newTestInstance();

        f.setPath("/");
        Cursor c = t.query(f, root);
        List<String> paths = new ArrayList<String>();
        while (c.hasNext()) {
            paths.add(c.next().getPath());
        }
        Collections.sort(paths);

        assertEquals(Arrays.asList(
                "/", "/children", "/children/c1", "/children/c2",
                "/children/c3", "/children/c4", "/parents",
                "/parents/p0", "/parents/p1",  "/parents/p2"),
                paths);
        assertFalse(c.hasNext());
        // endure it stays false
        assertFalse(c.hasNext());

        f.setPath("/nowhere");
        c = t.query(f, root);
        assertFalse(c.hasNext());
        // endure it stays false
        assertFalse(c.hasNext());
    }

}
