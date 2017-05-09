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
package org.apache.jackrabbit.oak.plugins.index.property;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.junit.Assert.assertNotNull;

public class PropertyIndexLookupTest {

    private static final List<String> PROP_NAMES = Lists.newArrayList("jcr:primaryType", "jcr:mixinTypes");
    private static final List<String> DECL_NODE_TYPES = Lists.newArrayList("nt:unstructured", "mix:versionable");

    private NodeState root;
    private NodeBuilder rootBuilder;
    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new PropertyIndexEditorProvider()));

    @Before
    public void setup() throws Exception {
        root = EmptyNodeState.EMPTY_NODE;
        rootBuilder = InitialContent.INITIAL_CONTENT.builder();
        commit();
    }

    @Test
    public void getIndexNodeForNamedDeclaringNodeTypes() throws Exception {
        rootBuilder.child(INDEX_DEFINITIONS_NAME).child("nodetype")
                .setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, DECL_NODE_TYPES, NAMES));
        commit();

        Filter f = createFilter(root, "nt:unstructured");
        assertNotNull("declaringNodeTypes with Name[] must find index node",
                new PropertyIndexLookup(root).getIndexNode(root, JCR_PRIMARYTYPE, f));
    }

    @Test
    public void getIndexNodeForStringDeclaringNodeTypes() throws Exception {
        rootBuilder.child(INDEX_DEFINITIONS_NAME).child("nodetype")
                .setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, DECL_NODE_TYPES, STRINGS));
        commit();

        Filter f = createFilter(root, "nt:unstructured");
        assertNotNull("declaringNodeTypes with String[] should also find index node",
                new PropertyIndexLookup(root).getIndexNode(root, JCR_PRIMARYTYPE, f));
    }

    @Test
    public void getIndexNodeForStringPropertyNames() throws Exception {
        rootBuilder.child(INDEX_DEFINITIONS_NAME).child("nodetype")
                .removeProperty(PROPERTY_NAMES)
                .setProperty(PropertyStates.createProperty(PROPERTY_NAMES, PROP_NAMES, STRINGS));
        commit();

        Filter f = createFilter(root, "nt:unstructured");
        assertNotNull("propertyNames with String[] should also find index node",
                new PropertyIndexLookup(root).getIndexNode(root, JCR_PRIMARYTYPE, f));
    }

    private void commit() throws Exception {
        root = HOOK.processCommit(rootBuilder.getBaseState(), rootBuilder.getNodeState(), EMPTY);
        rootBuilder = root.builder();
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]",
                new QueryEngineSettings());
    }
}
