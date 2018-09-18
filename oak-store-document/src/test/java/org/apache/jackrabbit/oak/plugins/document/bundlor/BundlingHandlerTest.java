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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BundlingHandlerTest {


    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void defaultSetup() throws Exception {
        BundlingHandler handler = new BundlingHandler(BundledTypesRegistry.from(EMPTY_NODE));
        childBuilder(builder, "/x/y/z");
        NodeState state = builder.getNodeState();

        assertEquals("/", handler.getRootBundlePath());
        assertTrue(handler.isBundlingRoot());
        assertEquals("foo", handler.getPropertyPath("foo"));

        BundlingHandler xh = childHandler(handler, state, "/x");
        assertEquals("/x", xh.getRootBundlePath());
        assertTrue(xh.isBundlingRoot());
        assertEquals("foo", xh.getPropertyPath("foo"));

        BundlingHandler xz = childHandler(handler, state, "/x/y/z");
        assertEquals("/x/y/z", xz.getRootBundlePath());
        assertTrue(xz.isBundlingRoot());
        assertEquals("foo", xz.getPropertyPath("foo"));
    }

    @Test
    public void ntFileBundled() throws Exception {
        BundledTypesRegistry registry = BundledTypesRegistry.builder().forType("nt:file", "jcr:content").buildRegistry();

        childBuilder(builder, "sunrise.jpg/jcr:content").setProperty("jcr:data", "foo");
        childBuilder(builder, "sunrise.jpg/jcr:content/bar").setProperty("jcr:data", "foo");
        type(childBuilder(builder, "sunrise.jpg"), "nt:file");
        childBuilder(builder, "/sunrise.jpg/metadata").setProperty("name", "foo");

        NodeState state = builder.getNodeState();
        BundlingHandler handler = new BundlingHandler(registry);

        BundlingHandler fileHandler = childHandler(handler, state, "/sunrise.jpg");
        assertEquals("/sunrise.jpg", fileHandler.getRootBundlePath());
        assertTrue(fileHandler.isBundlingRoot());
        assertFalse(fileHandler.isBundledNode());
        assertEquals("foo", fileHandler.getPropertyPath("foo"));

        BundlingHandler jcrContentHandler = childHandler(handler, state, "/sunrise.jpg/jcr:content");
        assertEquals("/sunrise.jpg", jcrContentHandler.getRootBundlePath());
        assertFalse(jcrContentHandler.isBundlingRoot());
        assertTrue(jcrContentHandler.isBundledNode());
        assertEquals("jcr:content/foo", jcrContentHandler.getPropertyPath("foo"));

        BundlingHandler metadataHandler = childHandler(handler, state, "/sunrise.jpg/metadata");
        assertEquals("/sunrise.jpg/metadata", metadataHandler.getRootBundlePath());
        assertTrue(metadataHandler.isBundlingRoot());
        assertFalse(metadataHandler.isBundledNode());
        assertEquals("foo", metadataHandler.getPropertyPath("foo"));

        // /sunrise.jpg/jcr:content/bar should have bundle root reset
        BundlingHandler barHandler = childHandler(handler, state, "/sunrise.jpg/jcr:content/bar");
        assertEquals("/sunrise.jpg/jcr:content/bar", barHandler.getRootBundlePath());
        assertTrue(barHandler.isBundlingRoot());
        assertEquals("foo", barHandler.getPropertyPath("foo"));
    }

    @Test
    public void childAdded_BundlingStart() throws Exception{
        BundledTypesRegistry registry = BundledTypesRegistry.builder().forType("nt:file", "jcr:content").buildRegistry();

        BundlingHandler handler = new BundlingHandler(registry);
        childBuilder(builder, "sunrise.jpg/jcr:content").setProperty("jcr:data", "foo");
        type(childBuilder(builder, "sunrise.jpg"), "nt:file");
        NodeState state = builder.getNodeState();

        BundlingHandler fileHandler = handler.childAdded("sunrise.jpg", state.getChildNode("sunrise.jpg"));
        assertEquals("/sunrise.jpg", fileHandler.getRootBundlePath());
        assertTrue(fileHandler.isBundlingRoot());
        assertEquals("foo", fileHandler.getPropertyPath("foo"));
        assertEquals(1, fileHandler.getMetaProps().size());
    }
    
    @Test
    public void childAdded_NoBundling() throws Exception{
        BundlingHandler handler = new BundlingHandler(BundledTypesRegistry.from(EMPTY_NODE));
        childBuilder(builder, "sunrise.jpg/jcr:content").setProperty("jcr:data", "foo");
        type(childBuilder(builder, "sunrise.jpg"), "nt:file");
        NodeState state = builder.getNodeState();

        BundlingHandler fileHandler = handler.childAdded("sunrise.jpg", state.getChildNode("sunrise.jpg"));
        assertEquals("/sunrise.jpg", fileHandler.getRootBundlePath());
        assertTrue(fileHandler.isBundlingRoot());
        assertEquals("foo", fileHandler.getPropertyPath("foo"));
        assertEquals(0, fileHandler.getMetaProps().size());
    }

    private BundlingHandler childHandler(BundlingHandler parent, NodeState parentState, String childPath) {
        BundlingHandler result = parent;
        NodeState state = parentState;
        for (String name : PathUtils.elements(checkNotNull(childPath))) {
            state = state.getChildNode(name);
            result = result.childAdded(name, state);
        }
        return result;
    }

    private NodeBuilder childBuilder(NodeBuilder parent, String childPath) {
        NodeBuilder result = parent;
        for (String name : PathUtils.elements(checkNotNull(childPath))) {
            result = result.child(name);
        }
        return result;
    }

    private NodeBuilder type(NodeBuilder builder, String typeName) {
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }
}