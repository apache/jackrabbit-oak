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

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CugImporterTest extends AbstractCugTest {

    private CugImporter importer;

    @Override
    public void before() throws Exception {
        super.before();
        importer = new CugImporter(Mounts.defaultMountInfoProvider());
    }

    @Test(expected = IllegalStateException.class)
    public void testInitTwice() throws Exception {
        Session session = Mockito.mock(Session.class);
        assertTrue(importer.init(session, root, getNamePathMapper(), true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW, new ReferenceChangeTracker(), getSecurityProvider()));
        importer.init(session, root, getNamePathMapper(), true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW, new ReferenceChangeTracker(), getSecurityProvider());
    }

    @Test
    public void testInvalidPropInfo() throws Exception {
        createCug(root, SUPPORTED_PATH, "principalName");
        Tree parent = root.getTree(SUPPORTED_PATH);
        PropInfo propInfo = new PropInfo(JcrConstants.JCR_PRIMARYTYPE, PropertyType.STRING, ImmutableList.of(new TextValue() {
            @Override
            public String getString() {
                return "principalName";
            }

            @Override
            public Value getValue(int targetType) throws RepositoryException {
                return getValueFactory(root).createValue("principalName", PropertyType.STRING);
            }

            @Override
            public void dispose() {
            }
        }));
        PropertyDefinition propDef = Mockito.mock(PropertyDefinition.class);
        assertFalse(importer.handlePropInfo(parent, propInfo, propDef));
    }
}