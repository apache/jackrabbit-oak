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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ReadPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadablePathsAccessControlTest extends AbstractPrincipalBasedTest {

    private Principal testPrincipal;
    private Iterator<String> readablePaths;
    private Iterator<String> readableChildPaths;

    private JackrabbitAccessControlManager acMgr;

    @Before
    public void before() throws Exception {
        super.before();

        acMgr = new PrincipalBasedAccessControlManager(getMgrProvider(root), getFilterProvider());

        testPrincipal = getTestSystemUser().getPrincipal();
        Set<String> paths = getConfig(AuthorizationConfiguration.class).getParameters().getConfigValue(PermissionConstants.PARAM_READ_PATHS, PermissionConstants.DEFAULT_READ_PATHS);
        assertFalse(paths.isEmpty());

        readablePaths = Iterators.cycle(Iterables.transform(paths, f -> getNamePathMapper().getJcrPath(f)));
        Set<String> childPaths = new HashSet<>();
        for (String path : paths) {
            Iterables.addAll(childPaths, Iterables.transform(root.getTree(path).getChildren(), tree -> getNamePathMapper().getJcrPath(tree.getPath())));
        }
        readableChildPaths = Iterators.cycle(childPaths);
    }

    private Subject getTestSubject() {
        return new Subject(true, Collections.singleton(testPrincipal), Set.of(), Set.of());
    }

    @Test
    public void testHasPrivilege() throws Exception {
        try (ContentSession cs = Java23Subject.doAsPrivileged(getTestSubject(), (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null)) {
            PrincipalBasedAccessControlManager testAcMgr = new PrincipalBasedAccessControlManager(getMgrProvider(cs.getLatestRoot()), getFilterProvider());

            Set<Principal> principals = Collections.singleton(testPrincipal);

            assertTrue(testAcMgr.hasPrivileges(readablePaths.next(), principals, privilegesFromNames(JCR_READ)));
            assertTrue(testAcMgr.hasPrivileges(readablePaths.next(), principals, privilegesFromNames(REP_READ_PROPERTIES)));

            assertTrue(testAcMgr.hasPrivileges(readableChildPaths.next(), principals, privilegesFromNames(REP_READ_NODES)));
            assertTrue(testAcMgr.hasPrivileges(readableChildPaths.next(), principals, privilegesFromNames(REP_READ_NODES, REP_READ_PROPERTIES)));
        }
    }

    @Test
    public void testNotHasPrivilege() throws Exception {
        try (ContentSession cs = Java23Subject.doAsPrivileged(getTestSubject(), (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null)) {
            PrincipalBasedAccessControlManager testAcMgr = new PrincipalBasedAccessControlManager(getMgrProvider(cs.getLatestRoot()), getFilterProvider());

            Set<Principal> principals = Collections.singleton(testPrincipal);

            assertFalse(testAcMgr.hasPrivileges(readablePaths.next(), principals, privilegesFromNames(JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)));
            assertFalse(testAcMgr.hasPrivileges(readablePaths.next(), principals, privilegesFromNames(PrivilegeConstants.JCR_ALL)));

            assertFalse(testAcMgr.hasPrivileges(readableChildPaths.next(), principals, privilegesFromNames(REP_READ_NODES, PrivilegeConstants.JCR_MODIFY_PROPERTIES)));
            assertFalse(testAcMgr.hasPrivileges(readableChildPaths.next(), principals, privilegesFromNames(REP_READ_NODES, REP_READ_PROPERTIES, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT)));
        }
    }

    @Test
    public void testHasPrivilegePrincipal() throws Exception {
        Set<Principal> principals = Collections.singleton(testPrincipal);

        assertTrue(acMgr.hasPrivileges(readablePaths.next(), principals, privilegesFromNames(JCR_READ)));
        assertTrue(acMgr.hasPrivileges(readablePaths.next(), principals, privilegesFromNames(REP_READ_PROPERTIES)));

        assertTrue(acMgr.hasPrivileges(readableChildPaths.next(), principals, privilegesFromNames(REP_READ_NODES)));
        assertTrue(acMgr.hasPrivileges(readableChildPaths.next(), principals, privilegesFromNames(REP_READ_NODES, REP_READ_PROPERTIES)));
    }

    @Test
    public void testNotHasPrivilegePrincipal() throws Exception {
        Set<Principal> principals = Collections.singleton(testPrincipal);

        assertFalse(acMgr.hasPrivileges(readablePaths.next(), principals, privilegesFromNames(JCR_READ, PrivilegeConstants.JCR_MODIFY_PROPERTIES)));
        assertFalse(acMgr.hasPrivileges(readablePaths.next(), principals, privilegesFromNames(PrivilegeConstants.JCR_ALL)));

        assertFalse(acMgr.hasPrivileges(readableChildPaths.next(), principals, privilegesFromNames(REP_READ_NODES, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)));
        assertFalse(acMgr.hasPrivileges(readableChildPaths.next(), principals, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT, REP_READ_PROPERTIES)));

        assertFalse(acMgr.hasPrivileges(ROOT_PATH, principals, privilegesFromNames(JCR_READ)));
        String systemPath = getNamePathMapper().getJcrPath(PathUtils.concat(ROOT_PATH, JcrConstants.JCR_SYSTEM));
        assertFalse(acMgr.hasPrivileges(systemPath, principals, privilegesFromNames(REP_READ_PROPERTIES)));
    }

    @Test
    public void testGetPrivileges() throws Exception {
        try (ContentSession cs = Java23Subject.doAsPrivileged(getTestSubject(), (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null)) {
            PrincipalBasedAccessControlManager testAcMgr = new PrincipalBasedAccessControlManager(getMgrProvider(cs.getLatestRoot()), getFilterProvider());

            Privilege[] expected = privilegesFromNames(JCR_READ);

            assertArrayEquals(expected, testAcMgr.getPrivileges(readablePaths.next()));
            assertArrayEquals(expected, testAcMgr.getPrivileges(readableChildPaths.next()));
        }
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetPrivilegesAtRoot() throws Exception {
        try (ContentSession cs = Java23Subject.doAsPrivileged(getTestSubject(), (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null)) {
            PrincipalBasedAccessControlManager testAcMgr = new PrincipalBasedAccessControlManager(getMgrProvider(cs.getLatestRoot()), getFilterProvider());
            testAcMgr.getPrivileges(ROOT_PATH);
        }
    }

    @Test
    public void testGetPrivilegesByPrincipal() throws Exception {
        Privilege[] expected = privilegesFromNames(JCR_READ);
        Set<Principal> principals = Collections.singleton(testPrincipal);

        assertArrayEquals(expected, acMgr.getPrivileges(readablePaths.next(), principals));
        assertArrayEquals(expected, acMgr.getPrivileges(readableChildPaths.next(), principals));

        assertEquals(0, acMgr.getPrivileges(ROOT_PATH, principals).length);
        assertEquals(0, acMgr.getPrivileges(PathUtils.concat(ROOT_PATH, getNamePathMapper().getJcrName(JcrConstants.JCR_SYSTEM)), principals).length);

    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        AccessControlPolicy[] expected = new AccessControlPolicy[] {ReadPolicy.INSTANCE};

        assertArrayEquals(expected, acMgr.getEffectivePolicies(readablePaths.next()));
        assertArrayEquals(expected, acMgr.getEffectivePolicies(readableChildPaths.next()));
    }

    @Test
    public void testGetEffectivePoliciesNullPath() throws Exception {
        assertEquals(0, acMgr.getEffectivePolicies((String) null).length);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetEffectivePoliciesLimitedAccess() throws Exception {
        try (ContentSession cs = Java23Subject.doAsPrivileged(getTestSubject(), (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null)) {
            PrincipalBasedAccessControlManager testAcMgr = new PrincipalBasedAccessControlManager(getMgrProvider(cs.getLatestRoot()), getFilterProvider());
            testAcMgr.getEffectivePolicies(readablePaths.next());
        }
    }

    @Test
    public void testGetEffectivePoliciesLimitedAccess2() throws Exception {
        String path = readablePaths.next();
        setupPrincipalBasedAccessControl(testPrincipal, path, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        // default: grant read-ac at root node as nodetype/namespace roots cannot have their mixin changed
        addDefaultEntry(PathUtils.ROOT_PATH, testPrincipal, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        root.commit();

        // test-session can read-ac at readable path but cannot access principal-based policy
        try (ContentSession cs = Java23Subject.doAsPrivileged(getTestSubject(), (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null)) {
            PrincipalBasedAccessControlManager testAcMgr = new PrincipalBasedAccessControlManager(getMgrProvider(cs.getLatestRoot()), getFilterProvider());
            Set<AccessControlPolicy> effective = Set.of(testAcMgr.getEffectivePolicies(path));

            assertEquals(1, effective.size());
            assertTrue(effective.contains(ReadPolicy.INSTANCE));
        }
    }

    @Test
    public void testGetEffectivePoliciesLimitedAccess3() throws Exception {
        String path = readablePaths.next();
        setupPrincipalBasedAccessControl(testPrincipal, path, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        setupPrincipalBasedAccessControl(testPrincipal, getTestSystemUser().getPath(), PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        // default: grant read and read-ac at root node to make sure both policies are accessible
        addDefaultEntry(PathUtils.ROOT_PATH, testPrincipal, PrivilegeConstants.JCR_READ_ACCESS_CONTROL, PrivilegeConstants.JCR_READ);
        root.commit();

        // test-session can read-ac at readable path and at principal-based policy
        try (ContentSession cs = Java23Subject.doAsPrivileged(getTestSubject(), (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null)) {
            PrincipalBasedAccessControlManager testAcMgr = new PrincipalBasedAccessControlManager(getMgrProvider(cs.getLatestRoot()), getFilterProvider());
            Set<AccessControlPolicy> effective = CollectionUtils.toSet(testAcMgr.getEffectivePolicies(path));

            assertEquals(2, effective.size());
            assertTrue(effective.remove(ReadPolicy.INSTANCE));
            assertTrue(effective.iterator().next() instanceof ImmutablePrincipalPolicy);
        }
    }

    @Test
    public void testGetEffectivePoliciesByPrincipal() throws Exception {
        // OAK-10135 : include read-policy in effective policies by principal result
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        assertEquals(1, effective.length);
        assertTrue(effective[0] instanceof ReadPolicy);
    }
}