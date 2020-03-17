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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.PROP_PATTERN;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DocumentBundlorTest {

    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void basicSetup() throws Exception{
        builder.setProperty(createProperty(PROP_PATTERN, asList("x", "x/y"), STRINGS));
        DocumentBundlor bundlor = DocumentBundlor.from(builder.getNodeState());

        assertTrue(bundlor.isBundled("x"));
        assertTrue(bundlor.isBundled("x/y"));
        assertFalse(bundlor.isBundled("x/y/z"));
        assertFalse(bundlor.isBundled("z"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void invalid() throws Exception{
        DocumentBundlor.from(builder.getNodeState());
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalid2() throws Exception{
        DocumentBundlor.from(Collections.<String>emptyList());
    }

    @Test
    public void asPropertyState() throws Exception{
        builder.setProperty(createProperty(PROP_PATTERN, asList("x", "x/y", "z"), STRINGS));
        DocumentBundlor bundlor = DocumentBundlor.from(builder.getNodeState());
        PropertyState ps = bundlor.asPropertyState();

        assertNotNull(ps);
        DocumentBundlor bundlor2 = DocumentBundlor.from(ps);
        assertTrue(bundlor2.isBundled("x"));
        assertTrue(bundlor2.isBundled("x/y"));
        assertFalse(bundlor2.isBundled("m"));
    }

}