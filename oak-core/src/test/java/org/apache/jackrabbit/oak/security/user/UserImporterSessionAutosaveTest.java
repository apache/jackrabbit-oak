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

import javax.jcr.ImportUUIDBehavior;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class UserImporterSessionAutosaveTest extends UserImporterTest {

    @Override
    public void before() throws Exception {
        super.before();

        getUserManager(root).autoSave(true);

    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userParams = ConfigurationParameters.of(
                ImmutableMap.of(
                        UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider,
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior(),
                        UserConstants.PARAM_SUPPORT_AUTOSAVE, Boolean.TRUE
                )
        );
        return ConfigurationParameters.of(UserConfiguration.NAME, userParams);
    }

    @Override
    public void after() throws Exception {
        try {
            UserManager uMgr = getUserManager(root);
            if (uMgr.isAutoSave()) {
                getUserManager(root).autoSave(false);
            }
        } finally {
            super.after();
        }
    }

    @Override
    boolean init(boolean createAction) throws Exception {
        getUserManager(root).autoSave(false);
        boolean b = super.init(createAction);
        getUserManager(root).autoSave(true);
        return b;
    }

    @Test
    public void testInitImportUUIDBehaviorRemove() throws Exception {
        assertFalse(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviorReplace() throws Exception {
        assertFalse(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviorThrow() throws Exception {
        assertFalse(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviourCreateNew() throws Exception {
        assertFalse(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW, new ReferenceChangeTracker(), getSecurityProvider()));
    }
}