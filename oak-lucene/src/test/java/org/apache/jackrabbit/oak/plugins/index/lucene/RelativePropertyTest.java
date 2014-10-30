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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RelativePropertyTest {
    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void relativeProperty() throws Exception{
        assertTrue(RelativeProperty.isRelativeProperty("foo/bar"));
        assertTrue(RelativeProperty.isRelativeProperty("foo/bar/baz"));
        assertFalse(RelativeProperty.isRelativeProperty("/foo/bar/baz"));
        assertFalse(RelativeProperty.isRelativeProperty("/"));
        assertFalse(RelativeProperty.isRelativeProperty(""));
    }

    @Test
    public void ancesstors() throws Exception{
        RelativeProperty rp = new RelativeProperty("foo/bar/baz/boom");
        assertArrayEquals(new String[] {"baz", "bar", "foo"}, rp.ancestors);
    }

    @Test
    public void getPropDefnNode() throws Exception{
        RelativeProperty rp = new RelativeProperty("foo/bar/baz");
        builder.child("foo").child("bar").child("baz").setProperty("a", "b");
        NodeBuilder propDefn = rp.getPropDefnNode(builder);
        assertTrue(propDefn.exists());
        assertEquals("b", propDefn.getString("a"));
    }

    @Test
    public void testProperty() throws Exception{
        RelativeProperty rp = new RelativeProperty("foo/bar/baz");
        builder.child("foo").child("bar").setProperty("baz", "b");
        PropertyState p = rp.getProperty(builder.getNodeState());
        assertNotNull(p);
        assertEquals("b", p.getValue(Type.STRING));

        RelativeProperty rp2 = new RelativeProperty("a/b");
        assertNull(rp2.getProperty(builder.getNodeState()));
    }
}
