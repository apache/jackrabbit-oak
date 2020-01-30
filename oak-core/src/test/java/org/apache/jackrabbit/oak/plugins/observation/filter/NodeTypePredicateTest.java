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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.JcrConstants.NT_RESOURCE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class NodeTypePredicateTest {

    @Test
    public void emptyNodeTypeList() {
        NodeState node = createNodeOfType(NT_BASE);
        TypePredicate p = new TypePredicate(node, new String[] {});
        assertFalse(p.apply(node));
    }

    @Test
    public void singleNodeTypeMatch() {
        NodeState node = createNodeOfType(NT_BASE);
        TypePredicate p = new TypePredicate(node, new String[] {NT_BASE});
        assertTrue(p.apply(node));
    }

    @Test
    public void singleNodeTypeMiss() {
        NodeState node = createNodeOfType(NT_BASE);
        TypePredicate p = new TypePredicate(node, new String[] {NT_FILE});
        assertFalse(p.apply(node));
    }

    @Test
    public void multipleNodeTypesMatch() {
        NodeState node = createNodeOfType(NT_FILE);
        TypePredicate p = new TypePredicate(node,
                new String[] { NT_FOLDER, NT_RESOURCE, NT_FILE });
        assertTrue(p.apply(node));
    }

    @Test
    public void multipleNodeTypesMiss() {
        NodeState node = createNodeOfType(NT_FILE);
        TypePredicate p = new TypePredicate(node,
                new String[] { NT_FOLDER, NT_RESOURCE, JCR_CONTENT });
        assertFalse(p.apply(node));
    }

    private static NodeState createNodeOfType(String ntName) {
        return EMPTY_NODE.builder()
                .setProperty(JCR_PRIMARYTYPE, ntName, Type.NAME)
                .getNodeState();
    }
}
