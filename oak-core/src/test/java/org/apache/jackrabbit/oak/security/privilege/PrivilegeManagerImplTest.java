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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.impl.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class PrivilegeManagerImplTest extends AbstractSecurityTest {

    private PrivilegeManagerImpl privilegeManager;

    @Before
    public void before() throws Exception {
        super.before();
        privilegeManager = create(root);
    }

    private static PrivilegeManagerImpl create(@Nonnull Root root) {
        return new PrivilegeManagerImpl(root, NamePathMapper.DEFAULT);
    }

    private static PrivilegeManagerImpl create(@Nonnull Root root, @Nonnull NamePathMapper mapper) {
        return new PrivilegeManagerImpl(root, mapper);
    }

    @Test
    public void testGetRegisteredPrivileges() throws RepositoryException {
        Privilege[] registered = privilegeManager.getRegisteredPrivileges();
        assertNotNull(registered);
        assertNotEquals(1, registered.length);
    }

    @Test
    public void testGetRegisteredPrivilegesFromEmptyRoot() throws RepositoryException {
        Privilege[] registered = create(getRootProvider().createReadOnlyRoot(EmptyNodeState.EMPTY_NODE)).getRegisteredPrivileges();
        assertNotNull(registered);
        assertEquals(0, registered.length);
    }

    @Test
    public void testGetPrivilege() throws Exception {
        Privilege p = privilegeManager.getPrivilege(PrivilegeConstants.JCR_VERSION_MANAGEMENT);

        assertNotNull(p);
        assertEquals(PrivilegeConstants.JCR_VERSION_MANAGEMENT, p.getName());
    }

    @Test(expected = AccessControlException.class)
    public void testGetPrivilegeExpandedNameMissingMapper() throws Exception {
        Privilege p = privilegeManager.getPrivilege(Privilege.JCR_VERSION_MANAGEMENT);

        assertNotNull(p);
        assertEquals(PrivilegeConstants.JCR_VERSION_MANAGEMENT, p.getName());
    }

    @Test
    public void testGetPrivilegeExpandedName() throws Exception {
        Privilege p = create(root, new NamePathMapperImpl(new GlobalNameMapper(root))).getPrivilege(Privilege.JCR_VERSION_MANAGEMENT);

        assertNotNull(p);
        assertNotEquals(Privilege.JCR_VERSION_MANAGEMENT, p.getName());
        assertEquals(PrivilegeConstants.JCR_VERSION_MANAGEMENT, p.getName());
    }

    @Test
    public void testGetPrivilegeRemappedNamespace() throws Exception {
        NamePathMapper mapper = new NamePathMapperImpl(new LocalNameMapper(root, ImmutableMap.of("prefix", NamespaceRegistry.NAMESPACE_JCR)));
        Privilege p = create(root, mapper).getPrivilege("prefix:read");

        assertNotNull(p);
        assertNotEquals(Privilege.JCR_READ, p.getName());
        assertNotEquals(PrivilegeConstants.JCR_READ, p.getName());
        assertEquals("prefix:read", p.getName());
    }

    @Test(expected = AccessControlException.class)
    public void testGetPrivilegeInvalidRemappedNamespace() throws Exception {
        NamePathMapper mapper = new NamePathMapperImpl(new LocalNameMapper(root, ImmutableMap.of("prefix", "unknownUri")));
        create(root, mapper).getPrivilege("prefix:read");
    }

    @Test(expected = AccessControlException.class)
    public void testGetPrivilegeFromEmptyRoot() throws Exception {
        create(getRootProvider().createReadOnlyRoot(EmptyNodeState.EMPTY_NODE)).getPrivilege(PrivilegeConstants.JCR_READ);
    }

    @Test(expected = AccessControlException.class)
    public void testGetUnknownPrivilege() throws Exception {
        create(getRootProvider().createReadOnlyRoot(EmptyNodeState.EMPTY_NODE)).getPrivilege("jcr:someName");
    }

    @Test(expected = AccessControlException.class)
    public void testGetPrivilegeEmptyName() throws Exception {
        create(getRootProvider().createReadOnlyRoot(EmptyNodeState.EMPTY_NODE)).getPrivilege("");
    }

    @Test(expected = AccessControlException.class)
    public void testGetPrivilegeNullName() throws Exception {
        create(getRootProvider().createReadOnlyRoot(EmptyNodeState.EMPTY_NODE)).getPrivilege(null);
    }

    @Test(expected = RepositoryException.class)
    public void testRegisterPrivilegePendingChanges() throws Exception {
        Root r = Mockito.mock(Root.class);
        when(r.hasPendingChanges()).thenReturn(true);
        create(r).registerPrivilege("privName", true, null);
    }

    @Test(expected = RepositoryException.class)
    public void testRegisterPrivilegeEmptyName() throws Exception {
        privilegeManager.registerPrivilege("", true, new String[]{"jcr:read", "jcr:write"});
    }

    @Test(expected = RepositoryException.class)
    public void testRegisterPrivilegeNullName() throws Exception {
        privilegeManager.registerPrivilege(null, true, new String[]{"jcr:read", "jcr:write"});
    }

    @Test(expected = RepositoryException.class)
    public void testRegisterPrivilegeUnknownAggreate() throws Exception {
        privilegeManager.registerPrivilege(null, true, new String[]{"unknown", "jcr:read"});
    }

    @Test(expected = RepositoryException.class)
    public void testRegisterPrivilegeReservedNamespace() throws Exception {
        privilegeManager.registerPrivilege("jcr:customPrivilege", true, new String[]{"jcr:read", "jcr:write"});
    }

    @Test(expected = RepositoryException.class)
    public void testRegisterPrivilegeReservedRemappedNamespace() throws Exception {
        NamePathMapper mapper = new NamePathMapperImpl(new LocalNameMapper(root, ImmutableMap.of("prefix", NamespaceRegistry.NAMESPACE_JCR)));
        PrivilegeManager pmgr = create(root, mapper);
        pmgr.registerPrivilege("prefix:customPrivilege", true, new String[] {"prefix:read", "prefix:write"});    }

    @Test
    public void testRegisterPrivilegeRemappedNamespace() throws Exception {
        ReadWriteNamespaceRegistry nsRegistry = new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };
        nsRegistry.registerNamespace("ns", "http://jackrabbit.apache.org/oak/ns");

        Map<String, String> localMapping = ImmutableMap.of(
                "prefix", NamespaceRegistry.NAMESPACE_JCR,
                "prefix2", "http://jackrabbit.apache.org/oak/ns");

        NamePathMapper mapper = new NamePathMapperImpl(new LocalNameMapper(root, localMapping));
        PrivilegeManager pmgr = create(root, mapper);

        Privilege p = pmgr.registerPrivilege("prefix2:customPrivilege", true, new String[] {"prefix:read", "prefix:write"});

        assertEquals("prefix2:customPrivilege", p.getName());
        assertEquals(2, p.getDeclaredAggregatePrivileges().length);

        Tree privilegesTree = root.getTree(PrivilegeConstants.PRIVILEGES_PATH);
        assertFalse(privilegesTree.hasChild("prefix2:customPrivilege"));

        Tree privTree = privilegesTree.getChild("ns:customPrivilege");
        assertTrue(privTree.exists());
        assertTrue(TreeUtil.getBoolean(privTree, PrivilegeConstants.REP_IS_ABSTRACT));

        Iterable<String> aggr = TreeUtil.getStrings(privTree, PrivilegeConstants.REP_AGGREGATES);
        assertNotNull(aggr);
        assertEquals(ImmutableSet.of("jcr:read", "jcr:write"), ImmutableSet.copyOf(aggr));

    }
}
