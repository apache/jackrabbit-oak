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

    public void testMixTitleOnUnstructured() throws Exception {
        Node n = testRootNode.addNode("unstructured", JcrConstants.NT_UNSTRUCTURED);
        n.addMixin("mix:title");
        superuser.save();

        // create jcr:title property with type LONG => declaring NT is nt:unstructured
        Property title = n.setProperty("jcr:title", 12345);
        assertEquals(PropertyType.LONG, title.getType());
        PropertyDefinition def = title.getDefinition();
        assertEquals(JcrConstants.NT_UNSTRUCTURED, def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.UNDEFINED, def.getRequiredType());

        // changing value to STRING => ValueFormatException expected
        try {
            title.setValue("str");
            fail();
        } catch (ValueFormatException e) {
            // success
        }

        // re-setting property to STRING -> change definition => declaring NT is mix:title
        n.setProperty("jcr:title", "str");
        assertEquals(PropertyType.STRING, title.getType());
        assertEquals(PropertyType.STRING, title.getValue().getType());
        def = title.getDefinition();
        assertEquals("mix:title", def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.STRING, def.getRequiredType());
    }

    public void testMixTitleOnUnstructured2() throws Exception {
        Node n = testRootNode.addNode("unstructured", JcrConstants.NT_UNSTRUCTURED);
        n.addMixin("mix:title");
        superuser.save();

        // create jcr:title as STRING => declaring NT is mix:title
        Property title = n.setProperty("jcr:title", "str");
        assertEquals(PropertyType.STRING, title.getType());
        PropertyDefinition def = title.getDefinition();
        assertEquals("mix:title", def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.STRING, def.getRequiredType());

        // changing value to BOOLEAN => value is converted
        title.setValue(true);
        assertEquals(PropertyType.STRING, title.getType());
        def = title.getDefinition();
        assertEquals("mix:title", def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.STRING, def.getRequiredType());

        // re-setting property to type BOOLEAN => declaring NT is nt:unstructured
        title = n.setProperty("jcr:title", true);
        def = title.getDefinition();
        assertEquals(JcrConstants.NT_UNSTRUCTURED, def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.UNDEFINED, def.getRequiredType());

        // same if property is set to type DOUBLE
        title = n.setProperty("jcr:title", superuser.getValueFactory().createValue(2.3));
        assertEquals(PropertyType.DOUBLE, title.getType());
        def = title.getDefinition();
        assertEquals(JcrConstants.NT_UNSTRUCTURED, def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.UNDEFINED, def.getRequiredType());

        // setting property to STRING => declaring NT is back to mix:title
        title = n.setProperty("jcr:title", "str");
        assertEquals(PropertyType.STRING, title.getType());
        def = title.getDefinition();
        assertEquals("mix:title", def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.STRING, def.getRequiredType());
    }

    public void testMixTitleOnFolder() throws Exception {
        Node n = testRootNode.addNode("folder", JcrConstants.NT_FOLDER);
        n.addMixin("mix:title");
        superuser.save();

        // create jcr:title property with type STRING (converted) => declaring NT is mix:title (and
        Property title = n.setProperty("jcr:title", 12345);
        assertEquals(PropertyType.STRING, title.getType());
        PropertyDefinition def = title.getDefinition();
        assertEquals("mix:title", def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.STRING, def.getRequiredType());

        // set value to BINARY -> changes value but not definition (binary gets converted)
        title.setValue(superuser.getValueFactory().createValue("abc", PropertyType.BINARY));
        assertEquals(PropertyType.STRING, title.getType());
        assertEquals(PropertyType.STRING, title.getValue().getType());
        def = title.getDefinition();
        assertEquals("mix:title", def.getDeclaringNodeType().getName());
        assertEquals(PropertyType.STRING, def.getRequiredType());

        superuser.save();
    }

    public void testLongRequiredTypeBoolean() throws Exception {
        try {
            Property p = node.setProperty(BOOLEAN_PROP_NAME, 12345);
            fail("Conversion from LONG to BOOLEAN must throw ValueFormatException");
        } catch (ValueFormatException e) {
            // success
        }
    }

    public void testPathRequiredTypeBoolean() throws Exception {
        Value pathValue = superuser.getValueFactory().createValue("/path", PropertyType.PATH);
        try {
            Property p = node.setProperty(BOOLEAN_PROP_NAME, pathValue);
            fail("Conversion from PATH to BOOLEAN must throw ValueFormatException");
        } catch (ValueFormatException e) {
            // success
        }
    }

    public void testStringRequiredTypeBoolean() throws Exception {
        Value stringValue = superuser.getValueFactory().createValue("true", PropertyType.STRING);
        Property p = node.setProperty(BOOLEAN_PROP_NAME, stringValue);
        assertEquals(PropertyType.BOOLEAN, p.getType());
        assertEquals(PropertyType.BOOLEAN, p.getValue().getType());
        assertTrue(p.getBoolean());
        PropertyDefinition def = p.getDefinition();
        assertEquals(PropertyType.BOOLEAN, def.getRequiredType());
        assertEquals(NT_NAME, def.getDeclaringNodeType().getName());
    }

    public void testRequiredTypeBooleanChangeDouble() throws Exception {
        Property p = node.setProperty(BOOLEAN_PROP_NAME, true);
        assertEquals(PropertyType.BOOLEAN, p.getType());
        assertEquals(PropertyType.BOOLEAN, p.getValue().getType());
        assertTrue(p.getBoolean());
        PropertyDefinition def = p.getDefinition();
        assertEquals(PropertyType.BOOLEAN, def.getRequiredType());
        assertEquals(NT_NAME, def.getDeclaringNodeType().getName());
        superuser.save();

        try {
            p.setValue(24.4);
            fail("Conversion from DOUBLE to BOOLEAN must throw ValueFormatException");
        } catch (ValueFormatException e) {
            // success
        }

        try {
            node.setProperty(BOOLEAN_PROP_NAME, 24.4);
            fail("Conversion from DOUBLE to BOOLEAN must throw ValueFormatException");
        } catch (ValueFormatException e) {
            // success
        }
    }

    public void testBooleanRequiredTypeLong() throws Exception {
        try {
            Property p = node.setProperty(LONG_PROP_NAME, true);
            fail("Conversion from BOOLEAN to LONG must throw ValueFormatException");
        } catch (ValueFormatException e) {
            // success
        }
    }

    public void testPathRequiredTypeLong() throws Exception {
        Value pathValue = superuser.getValueFactory().createValue("/path", PropertyType.PATH);
        try {
            Property p = node.setProperty(LONG_PROP_NAME, pathValue);
            fail("Conversion from PATH to LONG must throw ValueFormatException");
        } catch (ValueFormatException e) {
            // success
        }
    }

    public void testRequiredTypeLongChangeBoolean() throws Exception {
        Property p = node.setProperty(LONG_PROP_NAME, 12345);
        assertEquals(PropertyType.LONG, p.getType());
        assertEquals(PropertyType.LONG, p.getValue().getType());
        PropertyDefinition def = p.getDefinition();
        assertEquals(PropertyType.LONG, def.getRequiredType());
        assertEquals(NT_NAME, def.getDeclaringNodeType().getName());
        superuser.save();

        try {
            p.setValue(true);
            fail("Conversion from BOOLEAN to LONG must throw ValueFormatException");
        } catch (ValueFormatException e) {
            // success
        }
        try {
            node.setProperty(LONG_PROP_NAME, true);
            fail("Conversion from BOOLEAN to LONG must throw ValueFormatException");
        } catch (ValueFormatException e) {
            // success
        }
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