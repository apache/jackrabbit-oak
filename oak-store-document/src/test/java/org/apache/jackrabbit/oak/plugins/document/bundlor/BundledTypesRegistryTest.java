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

import java.util.Collections;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENMIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.PROP_PATTERN;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.*;

public class BundledTypesRegistryTest {

    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void basicSetup() throws Exception{
        builder.child("nt:file").setProperty(createProperty(PROP_PATTERN, asList("jcr:content"), STRINGS));
        BundledTypesRegistry registry = BundledTypesRegistry.from(builder.getNodeState());

        assertNull(registry.getBundlor(EMPTY_NODE));
        assertNotNull(registry.getBundlor(newNode("nt:file", false)));
        assertNull(registry.getBundlor(newNode("nt:resource", false)));

        DocumentBundlor bundlor = registry.getBundlor(newNode("nt:file", false));
        assertTrue(bundlor.isBundled("jcr:content"));
        assertFalse(bundlor.isBundled("foo"));
    }

    @Test
    public void disabledIgnored() throws Exception{
        builder.child("nt:file").setProperty(createProperty(PROP_PATTERN, asList("jcr:content"), STRINGS));
        builder.child("nt:file").setProperty(DocumentBundlor.PROP_DISABLED, true);
        BundledTypesRegistry registry = BundledTypesRegistry.from(builder.getNodeState());
        assertTrue(registry.getBundlors().isEmpty());
    }

    @Test
    public void mixin() throws Exception{
        builder.child("mix:foo").setProperty(createProperty(PROP_PATTERN, asList("jcr:content"), STRINGS));
        BundledTypesRegistry registry = BundledTypesRegistry.from(builder.getNodeState());
        assertNotNull(registry.getBundlor(newNode("mix:foo", true)));
    }

    @Test
    public void versioned() throws Exception{
        builder.child("nt:file").setProperty(createProperty(PROP_PATTERN, asList("jcr:content"), STRINGS));
        BundledTypesRegistry registry = BundledTypesRegistry.from(builder.getNodeState());

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, JcrConstants.NT_FROZENNODE, Type.NAME);
        builder.setProperty(JCR_FROZENPRIMARYTYPE, "nt:file", Type.NAME);

        assertNotNull(registry.getBundlor(builder.getNodeState()));
    }

    @Test
    public void versionedMixins() throws Exception{
        builder.child("mix:foo").setProperty(createProperty(PROP_PATTERN, asList("jcr:content"), STRINGS));
        BundledTypesRegistry registry = BundledTypesRegistry.from(builder.getNodeState());

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, JcrConstants.NT_FROZENNODE, Type.NAME);
        builder.setProperty(JCR_FROZENMIXINTYPES, Collections.singleton("mix:foo"), Type.NAMES);

        assertNotNull(registry.getBundlor(builder.getNodeState()));
    }


    @Test
    public void mixinOverPrimaryType() throws Exception{
        builder.child("mix:foo").setProperty(createProperty(PROP_PATTERN, asList("foo"), STRINGS));
        builder.child("nt:file").setProperty(createProperty(PROP_PATTERN, asList("jcr:content"), STRINGS));
        BundledTypesRegistry registry = BundledTypesRegistry.from(builder.getNodeState());

        NodeBuilder b2 = EMPTY_NODE.builder();
        setType("nt:file", false, b2);
        setType("mix:foo", true, b2);

        DocumentBundlor bundlor = registry.getBundlor(b2.getNodeState());

        //Pattern based on mixin would be applicable
        assertTrue(bundlor.isBundled("foo"));
        assertFalse(bundlor.isBundled("jcr:content"));
    }

    private static NodeState newNode(String typeName, boolean mixin){
        NodeBuilder builder = EMPTY_NODE.builder();
        setType(typeName, mixin, builder);
        return builder.getNodeState();
    }

    private static void setType(String typeName, boolean mixin, NodeBuilder builder) {
        if (mixin) {
            builder.setProperty(JCR_MIXINTYPES, Collections.singleton(typeName), Type.NAMES);
        } else {
            builder.setProperty(JCR_PRIMARYTYPE, typeName);
        }
    }


}