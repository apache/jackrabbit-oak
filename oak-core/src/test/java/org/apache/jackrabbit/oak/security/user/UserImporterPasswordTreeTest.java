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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.nodetype.PropertyDefinition;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserImporterPasswordTreeTest extends UserImporterBaseTest {

    private Tree pwTree;

    @Override
    public void before() throws Exception {
        super.before();

        init();
        Tree userTree = createUserTree();
        pwTree = TreeUtil.addChild(userTree, REP_PWD, NT_REP_PASSWORD);
    }

    @Test
    public void testNotRepPassword() throws Exception {
        pwTree.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);

        PropInfo propInfo = mock(PropInfo.class);
        PropertyDefinition propDef = mock(PropertyDefinition.class);
        assertFalse(importer.handlePropInfo(pwTree, propInfo, propDef));

        verify(propInfo, never()).getName();
        verify(propDef, never()).getRequiredType();
    }

    @Test
    public void testInvalidPropName() throws Exception {
        PropInfo propInfo = createPropInfo(null, "value");
        PropertyDefinition propDef = when(mock(PropertyDefinition.class).getName()).thenReturn(null).getMock();
        assertFalse(importer.handlePropInfo(pwTree, propInfo, propDef));
    }

    @Test
    public void testInvalidPropName2() throws Exception {
        PropInfo propInfo = createPropInfo(null, "value");
        PropertyDefinition propDef = when(mock(PropertyDefinition.class).getName()).thenReturn(NodeTypeConstants.RESIDUAL_NAME).getMock();
        assertFalse(importer.handlePropInfo(pwTree, propInfo, propDef));
    }

    @Test
    public void testLastModifiedUndefinedRequiredType() throws Exception {
        PropInfo propInfo = createPropInfo(REP_PASSWORD_LAST_MODIFIED, "23000");
        PropertyDefinition propDef = when(mock(PropertyDefinition.class).getRequiredType()).thenReturn(PropertyType.UNDEFINED).getMock();
        when(propDef.isMultiple()).thenReturn(false);

        assertTrue(importer.handlePropInfo(pwTree, propInfo, propDef));

        PropertyState ps = pwTree.getProperty(REP_PASSWORD_LAST_MODIFIED);
        assertNotNull(ps);
        assertSame(Type.LONG, ps.getType());
        assertEquals(23000, ps.getValue(Type.LONG).longValue());
        assertFalse(ps.isArray());
    }

    @Test
    public void testLastModifiedWithRequiredType() throws Exception {
        PropInfo propInfo = createPropInfo(REP_PASSWORD_LAST_MODIFIED, "23000");
        PropertyDefinition propDef = when(mock(PropertyDefinition.class).getRequiredType()).thenReturn(PropertyType.LONG).getMock();
        when(propDef.isMultiple()).thenReturn(true);

        assertTrue(importer.handlePropInfo(pwTree, propInfo, propDef));

        PropertyState ps = pwTree.getProperty(REP_PASSWORD_LAST_MODIFIED);
        assertNotNull(ps);
        assertSame(Type.LONGS, ps.getType());
        assertEquals(1, ps.count());
        assertEquals(23000, ps.getValue(Type.LONG, 0).longValue());
    }

    @Test
    public void testAnyPropNameUndefinedRequiredType() throws Exception {
        PropInfo propInfo = createPropInfo("any", "value");
        PropertyDefinition propDef = when(mock(PropertyDefinition.class).getRequiredType()).thenReturn(PropertyType.UNDEFINED).getMock();
        when(propDef.isMultiple()).thenReturn(true);

        assertTrue(importer.handlePropInfo(pwTree, propInfo, propDef));

        PropertyState ps = pwTree.getProperty("any");
        assertNotNull(ps);
        assertSame(Type.STRINGS, ps.getType());
        assertTrue(ps.isArray());
        assertEquals("value", ps.getValue(Type.STRING, 0));
    }

    @Test
    public void testAnyPropNameWithRequiredType() throws Exception {
        PropInfo propInfo = createPropInfo("any", "true");
        PropertyDefinition propDef = when(mock(PropertyDefinition.class).getRequiredType()).thenReturn(PropertyType.BOOLEAN).getMock();
        when(propDef.isMultiple()).thenReturn(false);

        assertTrue(importer.handlePropInfo(pwTree, propInfo, propDef));

        PropertyState ps = pwTree.getProperty("any");
        assertNotNull(ps);
        assertSame(Type.BOOLEAN, ps.getType());
        assertTrue(ps.getValue(Type.BOOLEAN));
    }
}