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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.PROP_PATTERN;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.*;

public class BundlingRootTest {
    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void defaultSetup() throws Exception{
        BundlingRoot root = new BundlingRoot();
        assertFalse(root.bundlingEnabled());
        assertEquals("foo", root.getPropertyPath("/x/y", "foo"));
        assertFalse(root.isBundled("x/y"));
    }

    @Test
    public void rootWithoutBundlor() throws Exception{
        BundlingRoot root = new BundlingRoot("/x/y", null);

        assertFalse(root.bundlingEnabled());
        assertEquals("foo", root.getPropertyPath("/x/y", "foo"));
        assertFalse(root.isBundled("x/y"));
        assertEquals("/x/y", root.getPath());
    }

    @Test
    public void rootWithBundlor() throws Exception{
        builder.setProperty(createProperty(PROP_PATTERN, asList("x", "x/y", "z/*;all", "z"), STRINGS));
        DocumentBundlor bundlor = DocumentBundlor.from(builder.getNodeState());

        BundlingRoot root = new BundlingRoot("/m/n", bundlor);
        assertTrue(root.bundlingEnabled());
        assertEquals("x/foo", root.getPropertyPath("/m/n/x", "foo"));
        assertEquals("x/y/foo", root.getPropertyPath("/m/n/x/y", "foo"));
        assertEquals("z/q/r/foo", root.getPropertyPath("/m/n/z/q/r", "foo"));

        assertEquals("foo", root.getPropertyPath("/m/n/k", "foo"));
        assertEquals("foo", root.getPropertyPath("/m/n/k/r", "foo"));
    }

}