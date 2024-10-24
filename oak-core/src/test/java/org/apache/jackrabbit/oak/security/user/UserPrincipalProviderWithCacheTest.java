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

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.principal.AbstractPrincipalProviderTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.security.user.CacheConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing the optional caching with the {@link org.apache.jackrabbit.oak.security.user.UserPrincipalProvider}.
 */
public class UserPrincipalProviderWithCacheTest extends AbstractPrincipalProviderTest {

    private String userId;

    private ContentSession systemSession;
    private Root systemRoot;

    @Override
    public void before() throws Exception {
        super.before();

        userId = getTestUser().getID();

        systemSession = getSystemSession();
        systemRoot = systemSession.getLatestRoot();
    }

    @Override
    public void after() throws Exception {
        try {
            if (systemSession != null) {
                systemSession.close();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(CacheConstants.PARAM_CACHE_EXPIRATION, 3600 * 1000)
        );
    }

    @NotNull
    @Override
    protected PrincipalProvider createPrincipalProvider() {
        return createPrincipalProvider(root);
    }

    private PrincipalProvider createPrincipalProvider(Root root) {
        return new UserPrincipalProvider(root, getUserConfiguration(), namePathMapper);
    }

    private ContentSession getSystemSession() throws Exception {
        if (systemSession == null) {
            systemSession = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null));
        }
        return systemSession;
    }

    private void changeUserConfiguration(ConfigurationParameters params) {
        UserConfiguration userConfig = getUserConfiguration();
        ((ConfigurationBase) userConfig).setParameters(params);
    }

    private Tree getCacheTree(Root root) throws Exception {
        return getCacheTree(root, getTestUser().getPath());
    }

    private Tree getCacheTree(Root root, String authorizablePath) {
        return root.getTree(authorizablePath + '/' + CacheConstants.REP_CACHE);
    }

    private static void assertPrincipals(Set<? extends Principal> principals, Principal... expectedPrincipals) {
        assertEquals(expectedPrincipals.length, principals.size());
        for (Principal principal : expectedPrincipals) {
            assertTrue(principals.contains(principal));
        }
    }

    @Test
    public void testGetPrincipalsPopulatesCache() throws Exception {
        PrincipalProvider pp = createPrincipalProvider(systemRoot);

        Set<? extends Principal> principals = pp.getPrincipals(userId);
        assertPrincipals(principals, EveryonePrincipal.getInstance(), testGroup.getPrincipal(), getTestUser().getPrincipal());

        root.refresh();

        Tree principalCache = getCacheTree(root);
        assertTrue(principalCache.exists());
        assertEquals(CacheConstants.NT_REP_CACHE, TreeUtil.getPrimaryTypeName(principalCache));

        assertNotNull(principalCache.getProperty(CacheConstants.REP_EXPIRATION));

        PropertyState ps = principalCache.getProperty(UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES);
        assertNotNull(ps);

        String val = ps.getValue(Type.STRING);
        assertEquals(testGroup.getPrincipal().getName(), val);
    }

    @Test
    public void testGetGroupMembershipPopulatesCache() throws Exception {
        PrincipalProvider pp = createPrincipalProvider(systemRoot);

        Set<? extends Principal> principals = pp.getMembershipPrincipals(getTestUser().getPrincipal());
        assertPrincipals(principals, EveryonePrincipal.getInstance(), testGroup.getPrincipal());

        root.refresh();

        Tree principalCache = getCacheTree(root);
        assertTrue(principalCache.exists());
        assertEquals(CacheConstants.NT_REP_CACHE, TreeUtil.getPrimaryTypeName(principalCache));

        assertNotNull(principalCache.getProperty(CacheConstants.REP_EXPIRATION));

        PropertyState ps = principalCache.getProperty(UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES);
        assertNotNull(ps);

        String val = ps.getValue(Type.STRING);
        assertEquals(testGroup.getPrincipal().getName(), val);
    }

    @Test
    public void testPrincipalManagerGetGroupMembershipPopulatesCache() throws Exception {
        PrincipalManager principalManager = getPrincipalManager(systemRoot);

        PrincipalIterator principalIterator = principalManager.getGroupMembership(getTestUser().getPrincipal());
        assertPrincipals(ImmutableSet.copyOf(principalIterator), EveryonePrincipal.getInstance(), testGroup.getPrincipal());

        root.refresh();

        Tree principalCache = getCacheTree(root);
        assertTrue(principalCache.exists());
        assertEquals(CacheConstants.NT_REP_CACHE, TreeUtil.getPrimaryTypeName(principalCache));

        assertNotNull(principalCache.getProperty(CacheConstants.REP_EXPIRATION));

        PropertyState ps = principalCache.getProperty(UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES);
        assertNotNull(ps);

        String val = ps.getValue(Type.STRING);
        assertEquals(testGroup.getPrincipal().getName(), val);
    }

    @Test
    public void testGetPrincipalsForGroups() throws Exception {
        PrincipalProvider pp = createPrincipalProvider(systemRoot);

        Set<? extends Principal> principals = pp.getPrincipals(testGroup.getID());
        assertTrue(principals.isEmpty());

        principals = pp.getPrincipals(testGroup2.getID());
        assertTrue(principals.isEmpty());

        root.refresh();

        Tree principalCache = getCacheTree(root, testGroup.getPath());
        assertFalse(principalCache.exists());

        principalCache = getCacheTree(root, testGroup2.getPath());
        assertFalse(principalCache.exists());
    }

    @Test
    public void testGetGroupMembershipForGroups() throws Exception {
        PrincipalProvider pp = createPrincipalProvider(systemRoot);

        Set<? extends Principal> principals = pp.getMembershipPrincipals(testGroup.getPrincipal());
        assertPrincipals(principals, EveryonePrincipal.getInstance());

        principals = pp.getMembershipPrincipals(testGroup2.getPrincipal());
        assertPrincipals(principals, EveryonePrincipal.getInstance(), testGroup.getPrincipal());

        root.refresh();

        Tree principalCache = getCacheTree(root, testGroup.getPath());
        assertFalse(principalCache.exists());

        principalCache = getCacheTree(root, testGroup2.getPath());
        assertFalse(principalCache.exists());
    }

    @Test
    public void testExtractPrincipalsFromCache() throws Exception {
        // a) force the cache to be created
        PrincipalProvider pp = createPrincipalProvider(systemRoot);

        // set of principals that read from user + membership-provider.
        Set<? extends Principal> principals = pp.getPrincipals(userId);
        assertPrincipals(principals, EveryonePrincipal.getInstance(), testGroup.getPrincipal(), getTestUser().getPrincipal());

        // b) retrieve principals again (this time from the cache)
        Set<? extends Principal> principalsAgain = pp.getPrincipals(userId);

        // make sure both sets are equal
        assertEquals(principals, principalsAgain);
    }

    @Test
    public void testGroupPrincipalNameEscape() throws Exception {
        String gId = null;
        try {
            Principal groupPrincipal = new PrincipalImpl(groupId + ",,%,%%");
            Group gr = getUserManager(root).createGroup(groupPrincipal);
            gId = gr.getID();
            gr.addMember(getTestUser());
            root.commit();
            systemRoot.refresh();

            PrincipalProvider pp = createPrincipalProvider(systemRoot);
            Set<? extends Principal> principals = pp.getPrincipals(userId);
            assertTrue(principals.contains(groupPrincipal));

            principals = pp.getPrincipals(userId);
            assertTrue(principals.contains(groupPrincipal));
        } finally {
            root.refresh();
            if (gId != null) {
                getUserManager(root).getAuthorizable(gId).remove();
                root.commit();
            }
        }
    }

    @Test
    public void testMembershipChange() throws Exception {
        PrincipalProvider pp = createPrincipalProvider(systemRoot);

        // set of principals that read from user + membership-provider.
        Set<? extends Principal> principals = pp.getPrincipals(userId);

        // change group membership with a different root
        UserManager uMgr = getUserManager(root);
        Group gr = uMgr.getAuthorizable(groupId, Group.class);
        assertTrue(gr.removeMember(uMgr.getAuthorizable(userId)));
        root.commit();
        systemRoot.refresh();

        // system-principal provider must still see the principals from the cache (not the changed onces)
        Set<? extends Principal> principalsAgain = pp.getPrincipals(userId);
        assertEquals(principals, principalsAgain);

        // disable the cache again
        changeUserConfiguration(ConfigurationParameters.EMPTY);
        pp = createPrincipalProvider(systemRoot);

        // now group principals must no longer be retrieved from the cache
        assertPrincipals(pp.getPrincipals(userId), EveryonePrincipal.getInstance(), getTestUser().getPrincipal());
    }

    @Test
    public void testCacheUpdate() throws Exception {
        //Reduce cache expiration time to 2ms to ensure cache is updated
        changeUserConfiguration(ConfigurationParameters.of(
                CacheConstants.PARAM_CACHE_EXPIRATION, 2
        ));
        PrincipalProvider pp = createPrincipalProvider(systemRoot);

        // set of principals that read from user + membership-provider -> cache being filled
        Set<? extends Principal> principals = pp.getPrincipals(userId);
        assertTrue(getCacheTree(systemRoot).exists());
        long expiration = TreeUtil.getLong(getCacheTree(systemRoot), CacheConstants.REP_EXPIRATION, 0);

        // change the group membership of the test user
        UserManager uMgr = getUserConfiguration().getUserManager(systemRoot, namePathMapper);
        Group gr = uMgr.getAuthorizable(groupId, Group.class);
        assertTrue(gr.removeMember(uMgr.getAuthorizable(userId)));
        systemRoot.commit();

        Thread.sleep(10); // wait for cache to expire

        // retrieve principals again to have cache updated
        pp = createPrincipalProvider(systemRoot);
        Set<? extends Principal> principalsAgain = pp.getPrincipals(userId);
        assertNotEquals(principals, principalsAgain);
        assertPrincipals(principalsAgain, EveryonePrincipal.getInstance(), getTestUser().getPrincipal());

        // verify that the cache has really been updated
        Tree cache = getCacheTree(systemRoot);
        assertNotSame(expiration, TreeUtil.getLong(cache, CacheConstants.REP_EXPIRATION, 0));
        assertEquals("", TreeUtil.getString(cache, UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES));

        // check that an cached empty membership set doesn't break the retrieval (OAK-8306)
        principalsAgain = pp.getPrincipals(userId);
        assertNotEquals(principals, principalsAgain);
        assertPrincipals(principalsAgain, EveryonePrincipal.getInstance(), getTestUser().getPrincipal());
    }

    @Test
    public void testOnlySystemCreatesCache() throws Exception {
        Set<? extends Principal> principals = principalProvider.getPrincipals(getTestUser().getID());
        assertPrincipals(principals, EveryonePrincipal.getInstance(), testGroup.getPrincipal(), getTestUser().getPrincipal());

        root.refresh();
        Tree userTree = root.getTree(getTestUser().getPath());

        assertFalse(userTree.hasChild(CacheConstants.REP_CACHE));
    }

    @Test
    public void testOnlySystemReadsFromCache() throws Exception {
        String userId = getTestUser().getID();

        PrincipalProvider systemPP = createPrincipalProvider(systemRoot);
        Set<? extends Principal> principals = systemPP.getPrincipals(userId);
        assertPrincipals(principals, EveryonePrincipal.getInstance(), testGroup.getPrincipal(), getTestUser().getPrincipal());

        root.refresh();
        assertPrincipals(principalProvider.getPrincipals(userId), EveryonePrincipal.getInstance(), testGroup.getPrincipal(), getTestUser().getPrincipal());

        testGroup.removeMember(getTestUser());
        root.commit();

        assertPrincipals(principalProvider.getPrincipals(userId), EveryonePrincipal.getInstance(), getTestUser().getPrincipal());
        assertPrincipals(systemPP.getPrincipals(userId), EveryonePrincipal.getInstance(), testGroup.getPrincipal(), getTestUser().getPrincipal());
    }

    @Test
    public void testInvalidExpiry() throws Exception {
        long[] noCache = new long[] {0, -1, Long.MIN_VALUE};
        for (long exp : noCache) {

            changeUserConfiguration(ConfigurationParameters.of(CacheConstants.PARAM_CACHE_EXPIRATION, exp));

            PrincipalProvider pp = createPrincipalProvider(systemRoot);
            pp.getPrincipals(userId);

            root.refresh();
            Tree userTree = root.getTree(getTestUser().getPath());
            assertFalse(userTree.hasChild(CacheConstants.REP_CACHE));
        }
    }

    @Test
    public void testLongOverflow() throws Exception {
        long[] maxCache = new long[] {Long.MAX_VALUE, Long.MAX_VALUE-1, Long.MAX_VALUE-10000};

        Root systemRoot = getSystemSession().getLatestRoot();
        for (long exp : maxCache) {
            changeUserConfiguration(ConfigurationParameters.of(CacheConstants.PARAM_CACHE_EXPIRATION, exp));

            PrincipalProvider pp = createPrincipalProvider(systemRoot);
            pp.getPrincipals(userId);

            Tree userTree = systemRoot.getTree(getTestUser().getPath());

            Tree cache = userTree.getChild(CacheConstants.REP_CACHE);
            assertTrue(cache.exists());

            PropertyState propertyState = cache.getProperty(CacheConstants.REP_EXPIRATION);
            assertNotNull(propertyState);
            assertEquals(Long.MAX_VALUE, propertyState.getValue(Type.LONG).longValue());

            cache.remove();
            systemRoot.commit();
        }
    }

    @Test
    public void testChangeCache() throws Exception {
        PrincipalProvider pp = createPrincipalProvider(systemRoot);
        pp.getPrincipals(userId);

        root.refresh();

        List<PropertyState> props = new ArrayList<>();
        props.add(PropertyStates.createProperty(CacheConstants.REP_EXPIRATION, 25));
        props.add(PropertyStates.createProperty(UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES, EveryonePrincipal.NAME));
        props.add(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED));
        props.add(PropertyStates.createProperty("residualProp", "anyvalue"));

        // changing cache with (normally) sufficiently privileged session and with system-session must not succeed
        for (Root r : new Root[] {root, systemRoot}) {
            for (PropertyState ps : props) {
                try {
                    Tree cache = getCacheTree(r);
                    cache.setProperty(ps);
                    r.commit();
                    fail("Attempt to modify the cache tree must fail.");
                } catch (CommitFailedException e) {
                    // success
                } finally {
                    r.refresh();
                }
            }
        }
    }

    @Test
    public void testRemoveCache() throws Exception {
        PrincipalProvider pp = createPrincipalProvider(systemRoot);
        pp.getPrincipals(userId);

        // removing cache with sufficiently privileged session must succeed
        root.refresh();
        Tree cache = getCacheTree(root);
        cache.remove();
        root.commit();
    }

    @Test
    public void testConcurrentLoginWithCacheRemoval() throws Exception {
        changeUserConfiguration(ConfigurationParameters.of(CacheConstants.PARAM_CACHE_EXPIRATION, 1));

        final List<Exception> exceptions = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            threads.add(new Thread(() -> {
                try {
                    login(new SimpleCredentials(userId, userId.toCharArray())).close();
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        for (Exception e : exceptions) {
            e.printStackTrace();
        }
        if (!exceptions.isEmpty()) {
            fail();
        }
    }
}
