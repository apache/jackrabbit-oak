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
package org.apache.jackrabbit.oak.jcr.nodetype;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("OAK-826") // FIXME
public class NodeDefinitionTest extends AbstractJCRTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        testRootNode.addNode("a", JcrConstants.NT_UNSTRUCTURED);
        testRootNode.addNode("b", JcrConstants.NT_FOLDER);
        superuser.save();
    }

    @Test
    public void testGetRequiredPrimaryTypes() throws RepositoryException {
        List<String> paths = new ArrayList();
        paths.add("/");
        paths.add("/jcr:system");
        paths.add("/jcr:system/jcr:versionStorage");
        paths.add("/jcr:system/jcr:nodeTypes");
        paths.add("/jcr:system/rep:namespaces");
        paths.add(testRoot + "/a");
        paths.add(testRoot + "/b");
        paths.add("/oak:index");

        for (String path : paths) {
            Node n = superuser.getNode(path);
            NodeDefinition def = n.getDefinition();
            def.getRequiredPrimaryTypes();
        }
    }

    @Test
    public void testGetRequiredPrimaryTypes2() throws RepositoryException {
        List<String> paths = new ArrayList();
        paths.add("/");
        paths.add("/jcr:system");
        paths.add("/jcr:system/jcr:versionStorage");
        paths.add("/jcr:system/jcr:nodeTypes");
        paths.add("/jcr:system/rep:namespaces");
        paths.add(testRoot + "/a");
        paths.add(testRoot + "/b");
        paths.add("/oak:index");

        for (String path : paths) {
            Node n = superuser.getNode(path);
            for (NodeDefinition nd : getAggregatedNodeDefinitions(n)) {
                nd.getRequiredPrimaryTypes();
            }
        }
    }


    private static NodeDefinition[] getAggregatedNodeDefinitions(Node node) throws RepositoryException {
        Set<NodeDefinition> cDefs = new HashSet();
        NodeDefinition[] nd = node.getPrimaryNodeType().getChildNodeDefinitions();
        cDefs.addAll(Arrays.asList(nd));
        NodeType[] mixins = node.getMixinNodeTypes();
        for (NodeType mixin : mixins) {
            nd = mixin.getChildNodeDefinitions();
            cDefs.addAll(Arrays.asList(nd));
        }
        return cDefs.toArray(new NodeDefinition[cDefs.size()]);
    }

    public static PropertyDefinition[] getAggregatedPropertyDefinitionss(Node node) throws RepositoryException {
        Set<PropertyDefinition> pDefs = new HashSet();
        PropertyDefinition pd[] = node.getPrimaryNodeType().getPropertyDefinitions();
        pDefs.addAll(Arrays.asList(pd));
        NodeType[] mixins = node.getMixinNodeTypes();
        for (NodeType mixin : mixins) {
            pd = mixin.getPropertyDefinitions();
            pDefs.addAll(Arrays.asList(pd));
        }
        return pDefs.toArray(new PropertyDefinition[pDefs.size()]);
    }
}