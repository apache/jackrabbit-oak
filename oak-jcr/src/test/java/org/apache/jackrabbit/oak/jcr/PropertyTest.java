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
package org.apache.jackrabbit.oak.jcr;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

public class PropertyTest extends AbstractJCRTest {

    private static final String NT_NAME = "PropertyTest_Type";
    private static final String BOOLEAN_PROP_NAME = "booleanProperty";
    private static final String LONG_PROP_NAME = "longProperty";
    private Node node;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        NodeTypeManager ntMgr = superuser.getWorkspace().getNodeTypeManager();
        NodeTypeTemplate template = ntMgr.createNodeTypeTemplate();
        template.setName(NT_NAME);
        template.setDeclaredSuperTypeNames(new String[] {JcrConstants.NT_BASE});
        template.setMixin(false);

        PropertyDefinitionTemplate pdt = ntMgr.createPropertyDefinitionTemplate();
        pdt.setName(BOOLEAN_PROP_NAME);
        pdt.setRequiredType(PropertyType.BOOLEAN);
        template.getPropertyDefinitionTemplates().add(pdt);

        pdt = ntMgr.createPropertyDefinitionTemplate();
        pdt.setName(LONG_PROP_NAME);
        pdt.setRequiredType(PropertyType.LONG);
        template.getPropertyDefinitionTemplates().add(pdt);

        ntMgr.registerNodeType(template, true);

        node = testRootNode.addNode(nodeName2, NT_NAME);
        assertEquals(NT_NAME, node.getPrimaryNodeType().getName());
    }

    public void testPropertyIteratorSize() throws Exception{
        Node n = testRootNode.addNode("unstructured", JcrConstants.NT_UNSTRUCTURED);
        n.setProperty("foo", "a");
        n.setProperty("foo-1", "b");
        n.setProperty("bar", "b");
        n.setProperty("cat", "c");
        superuser.save();

        //Extra 1 for jcr:primaryType
        PropertyIterator pitr = n.getProperties();
        assertEquals(4 + 1, pitr.getSize());
        assertEquals(4 + 1, Iterators.size(pitr));

        pitr = n.getProperties("foo*");
        assertEquals(2, pitr.getSize());
        assertEquals(2, Iterators.size(pitr));

        pitr = n.getProperties(new String[] {"foo*", "cat*"});
        assertEquals(3, pitr.getSize());
        assertEquals(3, Iterators.size(pitr));
    }
}
