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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.Principal;

import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MountPermissionProviderTest extends AbstractSecurityTest
        implements AccessControlConstants, PrivilegeConstants, PermissionConstants {

    private MountInfoProvider mountInfoProvider;
    private String testNode = "MultiplexingProviderTest";
    private String testPath = "/" + testNode;

    @Override
    @Before
    public void before() throws Exception {
        mountInfoProvider = Mounts.newBuilder().mount("testMount", testPath).build();
        super.before();
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Tree test = root.getTree(testPath);
            if (test.exists()) {
                test.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected SecurityProvider initSecurityProvider() {
        SecurityProvider sp = super.initSecurityProvider();
        AuthorizationConfiguration acConfig = sp.getConfiguration(AuthorizationConfiguration.class);
        Assert.assertTrue(acConfig instanceof CompositeAuthorizationConfiguration);
        ((AuthorizationConfigurationImpl) ((CompositeAuthorizationConfiguration) acConfig).getDefaultConfig())
                .bindMountInfoProvider(mountInfoProvider);
        return sp;
    }

    @Test
    public void multiplexingProvider() throws Exception {

        // check init
        Tree permStore = root.getTree(PERMISSIONS_STORE_PATH);
        String wsName = adminSession.getWorkspaceName();
        assertTrue(permStore.hasChild(wsName));
        for (Mount m : mountInfoProvider.getNonDefaultMounts()) {
            assertTrue(permStore.hasChild(MountPermissionProvider.getPermissionRootName(m, wsName)));
        }

        Tree rootNode = root.getTree("/");
        Tree test = TreeUtil.addChild(rootNode, testNode, JcrConstants.NT_UNSTRUCTURED);
        Tree content = TreeUtil.addChild(test, "content", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        Principal p = getTestUser().getPrincipal();
        setPrivileges(p, test.getPath(), true, JCR_READ);
        setPrivileges(p, content.getPath(), false, JCR_READ);

        permStore = root.getTree(PERMISSIONS_STORE_PATH);
        // no entries in the default store
        assertFalse(permStore.getChild(wsName).hasChild(p.getName()));
        for (Mount m : mountInfoProvider.getNonDefaultMounts()) {
            Tree mps = permStore.getChild(MountPermissionProvider.getPermissionRootName(m, wsName));
            assertTrue(mps.hasChild(p.getName()));
        }

        ContentSession testSession = createTestSession();
        try {
            Root r = testSession.getLatestRoot();
            assertFalse(r.getTree("/").exists());
            assertTrue(r.getTree(test.getPath()).exists());
            assertFalse(r.getTree(content.getPath()).exists());
        } finally {
            testSession.close();
        }
    }

    @Test
    public void multiplexingProviderOpen() throws Exception {

        Tree rootNode = root.getTree("/");
        Tree test = TreeUtil.addChild(rootNode, testNode, JcrConstants.NT_UNSTRUCTURED);
        Tree content = TreeUtil.addChild(test, "content", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        Principal p = getTestUser().getPrincipal();
        setPrivileges(p, "/", true, JCR_READ);
        setPrivileges(p, test.getPath(), false, JCR_READ);
        setPrivileges(p, content.getPath(), true, JCR_READ);

        ContentSession testSession = createTestSession();
        try {
            Root r = testSession.getLatestRoot();
            assertTrue(r.getTree("/").exists());
            assertFalse(test.getPath(), r.getTree(test.getPath()).exists());
            assertTrue(r.getTree(content.getPath()).exists());
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testPermissionProviderName() {
        assertEquals("oak.default",
                MountPermissionProvider.getPermissionRootName(mountInfoProvider.getDefaultMount(), "oak.default"));
        assertEquals("oak:mount-testMount-oak.default", MountPermissionProvider
                .getPermissionRootName(mountInfoProvider.getMountByName("testMount"), "oak.default"));
    }

    private void setPrivileges(Principal principal, String path, boolean allow, String... privileges) throws Exception {
        AccessControlManager acm = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acm, path);
        acl.addEntry(principal, privilegesFromNames(privileges), allow);
        acm.setPolicy(path, acl);
        root.commit();
    }
}
