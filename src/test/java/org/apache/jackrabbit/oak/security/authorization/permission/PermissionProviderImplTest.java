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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PermissionProviderImplTest extends AbstractSecurityTest implements AccessControlConstants {

    private static final String ADMINISTRATOR_GROUP = "administrators";
    private static final String[] READ_PATHS = new String[] {
            NamespaceConstants.NAMESPACES_PATH,
            NodeTypeConstants.NODE_TYPES_PATH,
            PrivilegeConstants.PRIVILEGES_PATH,
            "/test"
    };

    private Group adminstrators;

    @Override
    public void before() throws Exception {
        super.before();

        new NodeUtil(root.getTree("/")).addChild("test", JcrConstants.NT_UNSTRUCTURED);
        UserManager uMgr = getUserManager(root);
        adminstrators = uMgr.createGroup(ADMINISTRATOR_GROUP);
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            root.getTree("/test").remove();
            UserManager uMgr = getUserManager(root);
            if (adminstrators != null) {
                uMgr.getAuthorizable(adminstrators.getID()).remove();
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(PARAM_READ_PATHS, READ_PATHS);
        map.put(PARAM_ADMINISTRATOR_PRINCIPALS, new String[] {ADMINISTRATOR_GROUP});
        ConfigurationParameters acConfig = new ConfigurationParameters(map);

        return new ConfigurationParameters(ImmutableMap.of(AccessControlConfiguration.PARAM_ACCESS_CONTROL_OPTIONS, acConfig));
    }

    @Test
    public void testReadPath() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            Root r = testSession.getLatestRoot();
            Root immutableRoot = new ImmutableRoot(r, TreeTypeProvider.EMPTY);

            PermissionProvider pp = new PermissionProviderImpl(testSession.getLatestRoot(), testSession.getAuthInfo().getPrincipals(), getSecurityProvider());

            assertFalse(r.getTree("/").exists());
            assertSame(ReadStatus.DENY_THIS, pp.getReadStatus(immutableRoot.getTree("/"), null));

            for (String path : READ_PATHS) {
                assertTrue(r.getTree(path).exists());
                assertSame(ReadStatus.ALLOW_ALL_REGULAR, pp.getReadStatus(immutableRoot.getTree(path), null));
            }
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testAdministatorConfig() throws Exception {
        adminstrators.addMember(getTestUser());
        root.commit();

        ContentSession testSession = createTestSession();
        try {
            Root r = testSession.getLatestRoot();
            Root immutableRoot = new ImmutableRoot(r, TreeTypeProvider.EMPTY);

            PermissionProvider pp = new PermissionProviderImpl(testSession.getLatestRoot(), testSession.getAuthInfo().getPrincipals(), getSecurityProvider());

            assertTrue(r.getTree("/").exists());
            assertSame(ReadStatus.ALLOW_ALL, pp.getReadStatus(immutableRoot.getTree("/"), null));

            for (String path : READ_PATHS) {
                assertTrue(r.getTree(path).exists());
                assertSame(ReadStatus.ALLOW_ALL, pp.getReadStatus(immutableRoot.getTree(path), null));
            }
        } finally {
            testSession.close();
        }
    }
}