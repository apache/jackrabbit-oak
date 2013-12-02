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
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.junit.Test;

public class NodeTypePredicateTest {
    private final ReadOnlyNodeTypeManager ntManager =
            ReadOnlyNodeTypeManager.getInstance(InitialContent.INITIAL_CONTENT);

    @Test
    public void emptyNodeTypeList() {
        NodeTypePredicate p = new NodeTypePredicate(ntManager, new String[] {});
        ImmutableTree tree = createTreeOfType(NT_BASE);
        assertFalse(p.apply(tree));
    }

    @Test
    public void singleNodeTypeMatch() {
        NodeTypePredicate p = new NodeTypePredicate(ntManager, new String[] {NT_BASE});
        ImmutableTree tree = createTreeOfType(NT_BASE);
        assertTrue(p.apply(tree));
    }

    @Test
    public void singleNodeTypeMiss() {
        NodeTypePredicate p = new NodeTypePredicate(ntManager, new String[] {NT_FILE});
        ImmutableTree tree = createTreeOfType(NT_BASE);
        assertFalse(p.apply(tree));
    }

    @Test
    public void multipleNodeTypesMatch() {
        NodeTypePredicate p = new NodeTypePredicate(ntManager,
                new String[] { NT_FOLDER, NT_RESOURCE, NT_FILE });
        ImmutableTree tree = createTreeOfType(NT_FILE);
        assertTrue(p.apply(tree));
    }

    @Test
    public void multipleNodeTypesMiss() {
        NodeTypePredicate p = new NodeTypePredicate(ntManager,
                new String[] { NT_FOLDER, NT_RESOURCE, JCR_CONTENT });
        ImmutableTree tree = createTreeOfType(NT_FILE);
        assertFalse(p.apply(tree));
    }

    private static ImmutableTree createTreeOfType(String ntName) {
        return new ImmutableTree(EMPTY_NODE.builder()
                .setProperty(JCR_PRIMARYTYPE, ntName, Type.NAME)
                .getNodeState());
    }
}
