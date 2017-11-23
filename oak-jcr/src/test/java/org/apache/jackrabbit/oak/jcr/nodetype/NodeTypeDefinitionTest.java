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
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.ItemVisitor;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.util.TraversingItemVisitor;

import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

public class NodeTypeDefinitionTest extends AbstractJCRTest {

    public void testReadNodeTypeTree() throws Exception {
        String ntPath = NodeTypeConstants.NODE_TYPES_PATH + '/' + NodeTypeConstants.NT_UNSTRUCTURED;

        Node ntNode = superuser.getNode(ntPath);
        ItemVisitor visitor = new TraversingItemVisitor.Default() {
            @Override
            protected void entering(Node node, int level) throws RepositoryException {
                assertTrue(superuser.nodeExists(node.getPath()));
                super.entering(node, level);
            }
        };
        visitor.visit(ntNode);

    }

    public void testIndexedChildDefinition() throws Exception {
        String ntPath = NodeTypeConstants.NODE_TYPES_PATH + '/' + NodeTypeConstants.NT_VERSIONHISTORY;
        assertTrue(superuser.nodeExists(ntPath + "/jcr:childNodeDefinition"));
        assertTrue(superuser.nodeExists(ntPath + "/jcr:childNodeDefinition[1]"));

        Node cdNode = superuser.getNode(ntPath + "/jcr:childNodeDefinition[1]");
        assertEquals(ntPath + "/jcr:childNodeDefinition", cdNode.getPath());

        List<String> defNames = new ArrayList();
        NodeType nt = superuser.getWorkspace().getNodeTypeManager().getNodeType(NodeTypeConstants.NT_VERSIONHISTORY);
        for (NodeDefinition nd : nt.getDeclaredChildNodeDefinitions()) {
            defNames.add(nd.getName());
        }

        Node ntNode = superuser.getNode(ntPath);
        NodeIterator it = ntNode.getNodes("jcr:childNodeDefinition*");
        while (it.hasNext()) {
            Node def = it.nextNode();
            int index = getIndex(def);
            String name = (def.hasProperty(NodeTypeConstants.JCR_NAME)) ? def.getProperty(NodeTypeConstants.JCR_NAME).getString() : NodeTypeConstants.RESIDUAL_NAME;
            assertEquals(name, defNames.get(index-1));
        }
    }

    public void testIndexedPropertyDefinition() throws Exception {
        String ntPath = NodeTypeConstants.NODE_TYPES_PATH + '/' + NodeTypeConstants.NT_VERSION;

        assertTrue(superuser.nodeExists(ntPath + "/jcr:propertyDefinition"));
        assertTrue(superuser.nodeExists(ntPath + "/jcr:propertyDefinition[1]"));

        Node pdNode = superuser.getNode(ntPath + "/jcr:propertyDefinition[1]");
        assertEquals(ntPath + "/jcr:propertyDefinition", pdNode.getPath());

        List<String> defNames = new ArrayList();
        NodeType nt = superuser.getWorkspace().getNodeTypeManager().getNodeType(NodeTypeConstants.NT_VERSION);
        for (PropertyDefinition nd : nt.getDeclaredPropertyDefinitions()) {
            defNames.add(nd.getName());
        }

        Node ntNode = superuser.getNode(ntPath);
        NodeIterator it = ntNode.getNodes("jcr:propertyDefinition*");
        while (it.hasNext()) {
            Node def = it.nextNode();
            int index = getIndex(def);
            String name = (def.hasProperty(NodeTypeConstants.JCR_NAME)) ? def.getProperty(NodeTypeConstants.JCR_NAME).getString() : NodeTypeConstants.RESIDUAL_NAME;
            assertEquals(name, defNames.get(index-1));
        }
    }

    private static int getIndex(@Nonnull Node node) throws RepositoryException {
        String name = node.getName();
        int i = name.lastIndexOf('[');
        return (i == -1) ? 1 : Integer.valueOf(name.substring(i+1, name.lastIndexOf(']')));
    }
}