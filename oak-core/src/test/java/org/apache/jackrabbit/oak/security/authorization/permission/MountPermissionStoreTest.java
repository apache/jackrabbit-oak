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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class MountPermissionStoreTest extends AbstractSecurityTest {

    private static final String TEST_NAME = "MultiplexingProviderTest";
    private static final String TEST_PATH = "/" + TEST_NAME;
    private static final String CONTENT_NAME = "content";
    private static final String CONTENT_PATH = TEST_PATH + "/" + CONTENT_NAME;

    private MountInfoProvider mountInfoProvider = Mounts.newBuilder().mount("testMount", TEST_PATH).build();

    private AuthorizationConfiguration config;
    private PermissionStore permissionStore;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        Tree rootNode = root.getTree("/");
        Tree test = TreeUtil.addChild(rootNode, TEST_NAME, JcrConstants.NT_UNSTRUCTURED);
        Tree content = TreeUtil.addChild(test, CONTENT_NAME, JcrConstants.NT_UNSTRUCTURED);
        Tree child = TreeUtil.addChild(content, "child", JcrConstants.NT_UNSTRUCTURED);

        AccessControlManager acMgr = getAccessControlManager(root);
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_READ);

        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, content.getPath());
        assertNotNull(acl);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privileges);
        acMgr.setPolicy(content.getPath(), acl);

        AccessControlList acl2 = AccessControlUtils.getAccessControlList(acMgr, child.getPath());
        assertNotNull(acl2);
        acl2.addAccessControlEntry(EveryonePrincipal.getInstance(), privileges);
        acMgr.setPolicy(child.getPath(), acl2);
        root.commit();

        String wspName = adminSession.getWorkspaceName();
        PermissionProvider pp = config.getPermissionProvider(root, wspName, ImmutableSet.of(EveryonePrincipal.getInstance()));
        assertTrue(pp instanceof MountPermissionProvider);
        permissionStore = ((MountPermissionProvider) pp).getPermissionStore(root, wspName, RestrictionProvider.EMPTY);
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Tree test = root.getTree(TEST_PATH);
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
        config = MountUtils.bindMountInfoProvider(sp, mountInfoProvider);
        return sp;
    }

    @Test
    public void testLoadByAccessControlledPath() {
        Collection<PermissionEntry> entries = permissionStore.load(EveryonePrincipal.NAME, CONTENT_PATH);
        assertNotNull(entries);
        assertEquals(1, entries.size());
    }

    @Test
    public void testLoadByNonAccessControlledPath() {
        Collection<PermissionEntry> entries = permissionStore.load(EveryonePrincipal.NAME, TEST_PATH);
        assertNull(entries);
    }

    @Test
    public void testLoadByPrincipalNameWithEntries() {
        PrincipalPermissionEntries ppe = permissionStore.load(EveryonePrincipal.NAME);
        assertNotNull(ppe);
        assertTrue(ppe.isFullyLoaded());
        assertEquals(2, ppe.getSize());
    }

    @Test
    public void testLoadByUnknownPrincipalName() {
        PrincipalPermissionEntries ppe = permissionStore.load("unknown");
        assertNotNull(ppe);
        assertTrue(ppe.isFullyLoaded());
        assertEquals(0, ppe.getSize());
    }

    @Test
    public void testGetNumEntries() {
        assertEquals(2, permissionStore.getNumEntries(EveryonePrincipal.NAME, 10).size);
    }

    @Test
    public void testGetNumEntriesMaxReachedExact() throws Exception {
        PermissionStoreImpl mock = insertMockStore();
        when(mock.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(2, true));

        NumEntries ne = permissionStore.getNumEntries(EveryonePrincipal.NAME, 10);
        assertEquals(NumEntries.valueOf(4, true), ne);

        ne = permissionStore.getNumEntries(EveryonePrincipal.NAME, 2);
        assertEquals(NumEntries.valueOf(4, true), ne);
    }

    @Test
    public void testGetNumEntriesMaxReachedNotExact() throws Exception {
        PermissionStoreImpl mock = insertMockStore();
        when(mock.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(2, false));

        NumEntries ne = permissionStore.getNumEntries(EveryonePrincipal.NAME, 10);
        assertEquals(NumEntries.valueOf(4, false), ne);

        ne = permissionStore.getNumEntries(EveryonePrincipal.NAME, 2);
        assertEquals(NumEntries.valueOf(2, false), ne);
    }

    @Test
    public void testGetNumEntriesUnknownPrincipalName() {
        assertEquals(0, permissionStore.getNumEntries("unknown", 10).size);
    }

    @Test
    public void testFlush() throws Exception {
        PermissionStoreImpl mock = insertMockStore();

        permissionStore.flush(root);

        Mockito.verify(mock, Mockito.times(1)).flush(root);
    }

    private PermissionStoreImpl insertMockStore() throws Exception {
        Field f = Class.forName("org.apache.jackrabbit.oak.security.authorization.permission.MountPermissionProvider$MountPermissionStore").getDeclaredField("stores");
        f.setAccessible(true);

        PermissionStoreImpl mock = Mockito.mock(PermissionStoreImpl.class);
        List<PermissionStoreImpl> stores = (List<PermissionStoreImpl>) f.get(permissionStore);
        stores.add(0, mock);
        return mock;
    }
}