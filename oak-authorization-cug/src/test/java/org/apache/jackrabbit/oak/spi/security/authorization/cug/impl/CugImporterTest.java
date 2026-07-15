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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.junit.Test;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CugImporterTest extends AbstractCugTest {

    private CugImporter importer;

    @Override
    public void before() throws Exception {
        super.before();
        importer = new CugImporter(Mounts.defaultMountInfoProvider());
    }

    @Test(expected = IllegalStateException.class)
    public void testInitTwice() {
        Session session = mock(Session.class);
        assertTrue(importer.init(session, root, getNamePathMapper(), true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW, new ReferenceChangeTracker(), getSecurityProvider()));
        importer.init(session, root, getNamePathMapper(), true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW, new ReferenceChangeTracker(), getSecurityProvider());
    }

    @Test
    public void testInitFailsToRetrievePrincipalManager() throws Exception {
        JackrabbitSession s = mock(JackrabbitSession.class);
        when(s.getPrincipalManager()).thenThrow(new RepositoryException());

        assertFalse(importer.init(s, root, getNamePathMapper(), false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testHandlePropertyNonCugParent() throws Exception {
        createCug(root, SUPPORTED_PATH, "principalName");
        Tree nonCugParent = root.getTree(SUPPORTED_PATH);
        assertFalse(importer.handlePropInfo(nonCugParent, mock(PropInfo.class), mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropertyUnsupportedNamePropInfo() throws Exception {
        createCug(root, SUPPORTED_PATH, "principalName");
        Tree cugParent = root.getTree(SUPPORTED_PATH).getChild(REP_CUG_POLICY);
        PropInfo propInfo = when(mock(PropInfo.class).getName()).thenReturn("unsupportedPropName").getMock();
        assertFalse(importer.handlePropInfo(cugParent, propInfo, mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropertySingleValuePropDef() throws Exception {
        createCug(root, SUPPORTED_PATH, "principalName");
        Tree cugParent = root.getTree(SUPPORTED_PATH).getChild(REP_CUG_POLICY);

        PropInfo propInfo = when(mock(PropInfo.class).getName()).thenReturn(REP_PRINCIPAL_NAMES).getMock();

        PropertyDefinition propDef = when(mock(PropertyDefinition.class).isMultiple()).thenReturn(false).getMock();
        assertFalse(importer.handlePropInfo(cugParent, propInfo, propDef));
    }

    @Test
    public void testHandlePropertyPropDefWithWrongDeclaringNt() throws Exception {
        createCug(root, SUPPORTED_PATH, "principalName");
        Tree cugParent = root.getTree(SUPPORTED_PATH).getChild(REP_CUG_POLICY);

        PropInfo propInfo = when(mock(PropInfo.class).getName()).thenReturn(REP_PRINCIPAL_NAMES).getMock();

        NodeType nt = when(mock(NodeType.class).getName()).thenReturn(NodeTypeConstants.NT_UNSTRUCTURED).getMock();

        PropertyDefinition propDef = mock(PropertyDefinition.class);
        when(propDef.isMultiple()).thenReturn(true);
        when(propDef.getDeclaringNodeType()).thenReturn(nt);

        assertFalse(importer.handlePropInfo(cugParent, propInfo, propDef));
    }
}