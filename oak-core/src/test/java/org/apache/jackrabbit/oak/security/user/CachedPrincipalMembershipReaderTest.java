/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.security.user;

import static org.apache.jackrabbit.oak.security.user.CachedPrincipalMembershipReader.CacheConfiguration.NO_STALE_CACHE;
import static org.apache.jackrabbit.oak.security.user.CachedPrincipalMembershipReader.CacheConfiguration.PARAM_CACHE_EXPIRATION;
import static org.apache.jackrabbit.oak.security.user.CachedPrincipalMembershipReader.CacheConfiguration.PARAM_CACHE_MAX_STALE;
import static org.apache.jackrabbit.oak.security.user.MembershipCacheConstants.REP_CACHE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.security.user.CachedPrincipalMembershipReader.CacheConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.event.Level;

@RunWith(Parameterized.class)
public class CachedPrincipalMembershipReaderTest extends AbstractSecurityTest {

    private Function<Tree, Set<Principal>> cacheLoader;

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return Lists.newArrayList(
                new Object[]{10000L, "Cache Max Stale = 10000"},
                new Object[]{NO_STALE_CACHE, "Cache Max Stale = 0"});
    }

    private static final int NUM_THREADS = 100;

    private final long cacheMaxStale;

    private final LogCustomizer logCustomizer = LogCustomizer.forLogger(CachedPrincipalMembershipReader.class).enable(Level.DEBUG).create();

    private ContentSession systemSession;
    private String userPath;
    private Root mockedRoot;
    private Tree mockedUser;
    private Function<String, Principal> mockedGroupPrincipalFactory;
    String groupId;
    String groupId2;

    public CachedPrincipalMembershipReaderTest(long cacheMaxStale) {
        super();
        this.cacheMaxStale = cacheMaxStale;
    }

    @Override
    public void before() throws Exception {
        super.before();

        groupId = "testGroup" + UUID.randomUUID();
        Group testGroup = getUserManager(root).createGroup(groupId);
        testGroup.addMember(getTestUser());

        groupId2 = "testGroup" + UUID.randomUUID() + "2";
        Group testGroup2 = getUserManager(root).createGroup(groupId2);
        testGroup.addMember(testGroup2);

        root.commit();

        userPath = getTestUser().getPath();
        logCustomizer.starting();

        cacheLoader = membershipLoader(root, getUserConfiguration());

    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            String[] rm = new String[]{groupId, groupId2};
            for (String id : rm) {
                Authorizable a = getUserManager(root).getAuthorizable(id);
                if (a != null) {
                    a.remove();
                    root.commit();
                }
            }

            if (systemSession != null) {
                systemSession.close();
            }
        } finally {
            logCustomizer.finished();
            super.after();
        }
    }

    @NotNull CachedPrincipalMembershipReader createCacheMembershipReader(@NotNull Root root) {
        return createCacheMembershipReader(root, getUserConfiguration());
    }

    @NotNull CachedPrincipalMembershipReader createCacheMembershipReader(@NotNull Root root,
            @NotNull UserConfiguration userConfiguration) {

        return new CachedPrincipalMembershipReader(
                getCacheConfiguration(userConfiguration),
                root,
                name -> getPrincipalByName(root, name));
    }

    private @NotNull Function<Tree, Set<Principal>> membershipLoader(@NotNull Root root,
            @NotNull UserConfiguration userConfiguration) {
        return (Tree tree) -> {
            MembershipProvider membershipProvider = new MembershipProvider(root, userConfiguration.getParameters());
            Set<Principal> groupPrincipals = new HashSet<>();
            Iterator<Tree> groupTrees = membershipProvider.getMembership(tree, true);
            while (groupTrees.hasNext()) {
                Tree groupTree = groupTrees.next();
                if (UserUtil.isType(groupTree, AuthorizableType.GROUP)) {
                    groupPrincipals.add(getPrincipalByName(root, groupTree.getName()));
                }
            }
            return groupPrincipals;
        };
    }

    private static CacheConfiguration getCacheConfiguration(@NotNull UserConfiguration userConfiguration) {
        return CacheConfiguration.fromUserConfiguration(userConfiguration);
    }

    CachedPrincipalMembershipReader createMockedPrincipalMembershipReader(Function<String, Principal> groupPrincipalFactory,
            UserConfiguration userConfiguration, Root root) {
        CacheConfiguration cacheConfiguration = getCacheConfiguration(userConfiguration);
        return new CachedPrincipalMembershipReader(cacheConfiguration, root, groupPrincipalFactory);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(
                        PARAM_CACHE_EXPIRATION, 50000,
                        PARAM_CACHE_MAX_STALE, cacheMaxStale
                )
        );
    }

    private Root getSystemRoot() throws Exception {
        if (systemSession == null) {
            systemSession = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null));
        }
        return systemSession.getLatestRoot();
    }

    @Test
    public void testWritingCacheFailsWithException() throws Exception {
        CachedPrincipalMembershipReader cachedGroupMembershipReader = createCacheMembershipReader(root);

        Set<Principal> groupPrincipals = new HashSet<>();
        groupPrincipals.addAll(cachedGroupMembershipReader.readMembership(root.getTree(userPath), cacheLoader));

        Set<Principal> expected = Collections.singleton(getUserManager(root).getAuthorizable(groupId).getPrincipal());
        assertEquals(expected, groupPrincipals);

        assertEquals(2, logCustomizer.getLogs().size());
        assertEquals("Attempting to create new membership cache at " + userPath, logCustomizer.getLogs().get(0));
        assertEquals("Failed to cache membership: OakConstraint0034: Attempt to create or change the system maintained cache.", logCustomizer.getLogs().get(1));
    }

    /**
     * This test checks that 'readMembership' works for objects that are not users but doesn't cache them
     * @throws Exception
     */
    @Test
    public void testReadMembershipForNonUser() throws Exception {
        CachedPrincipalMembershipReader cachedGroupMembershipReader = spy(createCacheMembershipReader(root));

        Set<Principal> groupPrincipals = new HashSet<>();
        Tree groupTree = getTree(groupId2, root);
        groupPrincipals.addAll(cachedGroupMembershipReader.readMembership(groupTree, cacheLoader));

        Set<Principal> expected = Collections.singleton(getUserManager(root).getAuthorizable(groupId).getPrincipal());
        assertEquals(expected, groupPrincipals);

        assertEquals(0, logCustomizer.getLogs().size());
        verify(cachedGroupMembershipReader).readMembership(groupTree, cacheLoader);
        verifyNoMoreInteractions(cachedGroupMembershipReader);
    }

    /**
     * This test checks that cache is properly read and written
     */
    @Test
    public void testReadMembershipWithCache() throws Exception {
        Root systemRoot = getSystemRoot();
        CachedPrincipalMembershipReader cachedGroupMembershipReader = createCacheMembershipReader(systemRoot);

        Set<Principal> groupPrincipal = new HashSet<>();
        groupPrincipal.addAll(cachedGroupMembershipReader.readMembership(systemRoot.getTree(userPath), cacheLoader));

        //Assert that the first time the cache was created
        assertEquals(2, logCustomizer.getLogs().size());
        assertEquals("Attempting to create new membership cache at " + userPath, logCustomizer.getLogs().get(0));
        assertEquals(1, groupPrincipal.size());

        groupPrincipal.addAll(cachedGroupMembershipReader.readMembership(systemRoot.getTree(userPath), cacheLoader));
        assertEquals(3, logCustomizer.getLogs().size());
        //Assert that the cache was used
        assertEquals("Reading membership from cache for '" + userPath + "'", logCustomizer.getLogs().get(2));
        assertEquals(1, groupPrincipal.size());
    }

    /**
     * This test checks the behavior for an expired cache when the commit is 3 second longs and NUM_THREADS threads execute readMembership:
     * - if CacheConfiguration.PARAM_CACHE_MAX_STALE is 0, no stale cache should be provided during a long commit when
     * - if CacheConfiguration.PARAM_CACHE_MAX_STALE is >0, the stale cache must be returned during a long commit.
     */
    @Test
    public void testCacheGroupMembershipGetMemberStaleCache() throws Exception {
        initMocks();

        Principal principal = mock(Principal.class);
        when(principal.getName()).thenReturn("groupPrincipal");

        // Test getMembership from multiple threads on expired cache and verify that:
        // - only one thread updated the cache
        // - the stale value was provided
        Thread[] getMembershipThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < getMembershipThreads.length; i++) {
            getMembershipThreads[i] = new Thread(() -> {
                CachedPrincipalMembershipReader membershipReader = createMockedPrincipalMembershipReader(mockedGroupPrincipalFactory, getUserConfiguration(), mockedRoot);
                Set<Principal> groupPrincipals = new HashSet<>(membershipReader.readMembership(mockedUser, (tree) -> Set.of(principal)));
                assertEquals(groupPrincipals.size(), 1);
            });
            getMembershipThreads[i].start();
        }
        for (Thread getMembershipThread : getMembershipThreads) {
            getMembershipThread.join();
        }

        verify(mockedRoot, times(1)).commit(CacheValidatorProvider.asCommitAttributes());
        if (cacheMaxStale == NO_STALE_CACHE) {
            assertEquals(NUM_THREADS, logCustomizer.getLogs().size());
            logCustomizer.getLogs().subList(0, NUM_THREADS - 1).forEach(s -> assertEquals("Another thread is updating the cache and this thread is not allowed to serve a stale cache; reading from persistence without caching.", s));
        } else {
            assertEquals(NUM_THREADS, logCustomizer.getLogs().size());
            logCustomizer.getLogs().subList(0, NUM_THREADS - 1).forEach(s -> assertEquals("Another thread is updating the cache, returning a stale cache for '" + mockedUser.getPath() + "'.", s));
        }
        assertTrue(logCustomizer.getLogs().get(NUM_THREADS - 1).startsWith("Cached membership property 'rep:groupPrincipalNames' at " + mockedUser.getPath()));
    }

    private void initMocks() throws CommitFailedException {
        mockedRoot = mock(Root.class);
        doAnswer(invocation -> {
            Thread.sleep(3000);
            return null;
        }).when(mockedRoot).commit(CacheValidatorProvider.asCommitAttributes());

        MembershipProvider mockedMembershipProvider = mock(MembershipProvider.class);

        // Mock user Tree
        mockedUser = mock(Tree.class);
        PropertyState userState = mock(PropertyState.class);
        when(userState.getValue(any())).thenReturn(UserConstants.NT_REP_USER);
        when(mockedUser.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(userState);
        when(mockedUser.getPath()).thenReturn(UserConstants.DEFAULT_USER_PATH + "/test");

        // Mock Cache Tree
        Tree mockedPrincipalCache = mock(Tree.class);
        when(mockedPrincipalCache.exists()).thenReturn(true);

        PropertyState propertyStateExpiration = mock(PropertyState.class);
        when(propertyStateExpiration.getValue(Type.LONG)).thenReturn(System.currentTimeMillis());
        when(mockedPrincipalCache.getProperty(MembershipCacheConstants.REP_EXPIRATION)).thenReturn(
                propertyStateExpiration);

        PropertyState propertyStatePrincipalNames = mock(PropertyState.class);
        when(propertyStatePrincipalNames.getValue(Type.STRING)).thenReturn("groupPrincipal");
        when(mockedPrincipalCache.getProperty(MembershipCacheConstants.REP_GROUP_PRINCIPAL_NAMES)).thenReturn(
                propertyStatePrincipalNames);

        when(mockedUser.getChild(REP_CACHE)).thenReturn(mockedPrincipalCache);

        // Mock Group Tree
        Tree mockedGroupTree = mock(Tree.class);
        PropertyState groupTreeState = mock(PropertyState.class);
        when(groupTreeState.getValue(any())).thenReturn(UserConstants.NT_REP_GROUP);
        when(mockedGroupTree.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(groupTreeState);
        when(mockedGroupTree.getProperty(UserConstants.REP_PRINCIPAL_NAME)).thenReturn(groupTreeState);
        when(mockedMembershipProvider.getMembership(mockedUser, true)).thenAnswer(I -> Arrays.asList(new Tree[]{mockedGroupTree}).iterator());
        Principal groupPrincipal = mock(Principal.class);
        when(groupPrincipal.getName()).thenReturn("groupPrincipal");
        mockedGroupPrincipalFactory = (s) -> groupPrincipal;
    }

    @Test
    public void testMultipleUserConfigurations() throws Exception {
        Root systemRoot = getSystemRoot();

        List<UserConfiguration> userConfigurations = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            UserConfiguration uc = new UserConfigurationImpl(getSecurityProvider());
            userConfigurations.add(uc);

            CachedPrincipalMembershipReader cachedGroupMembershipReader = createCacheMembershipReader(systemRoot, uc);
            cachedGroupMembershipReader.readMembership(systemRoot.getTree(userPath), cacheLoader);
        }

        Field f = CachedPrincipalMembershipReader.class.getDeclaredField("CACHE_UPDATES");
        f.setAccessible(true);

        // verify that the size of the update-cache map is limited to the defined maxCacheTrackingEntries size
        Map<UserConfiguration, Map<String, Long>> cacheUpdates = (Map) f.get(createCacheMembershipReader(systemRoot));
        assertTrue(cacheUpdates.keySet().removeAll(userConfigurations));
    }

    @Test
    public void testMaxCacheTrackingEntries() throws Exception {
        UserConfiguration uc = new UserConfigurationImpl(securityProvider);
        Map<UserConfiguration, Map<String, Long>> cacheUpdates = null;
        try {
            Root systemRoot = getSystemRoot();
            CachedPrincipalMembershipReader membershipReader = createCacheMembershipReader(systemRoot, uc);
            membershipReader.readMembership(systemRoot.getTree(userPath), cacheLoader);

            Field f = CachedPrincipalMembershipReader.class.getDeclaredField("CACHE_UPDATES");
            f.setAccessible(true);

            cacheUpdates = (Map) f.get(membershipReader);

            Map<String, Long> m = cacheUpdates.get(uc);
            // since the cache entry is removed upon completion of readMembership -> expected size = 0
            assertEquals(0, m.size());

            for (int i = 0; i < 200; i++) {
                m.computeIfAbsent("/user/path/" + i, key -> System.currentTimeMillis());
            }

            int maxCacheTrackingEntries = 100; // constant in CachedPrincipalMembershipReader
            assertEquals(maxCacheTrackingEntries, m.size());

        } finally {
            if (cacheUpdates != null) {
                cacheUpdates.remove(uc);
            }
        }
    }

    @Test
    public void testMultipleCacheProviderWithDifferentProperties() throws Exception {
        Root systemRoot = getSystemRoot();
        CachedPrincipalMembershipReader defaultCacheReader = createCacheMembershipReader(systemRoot);

        //Create cache configuration but targeting different property name
        String newCachePropertyName = "anotherCache";
        CacheConfiguration anotherCacheConfiguration = CacheConfiguration.fromUserConfiguration(getUserConfiguration(),
                newCachePropertyName);
        CachedPrincipalMembershipReader anotherCacheReader = new CachedPrincipalMembershipReader(anotherCacheConfiguration,
                systemRoot, name -> getPrincipalByName(systemRoot, name));

        Set<Principal> groupPrincipal = new HashSet<>();
        groupPrincipal.addAll(defaultCacheReader.readMembership(systemRoot.getTree(userPath), cacheLoader));

        //Assert that the first time the cache was created
        assertEquals(2, logCustomizer.getLogs().size());
        assertEquals("Attempting to create new membership cache at " + userPath, logCustomizer.getLogs().get(0));
        assertEquals(1, groupPrincipal.size());

        groupPrincipal.addAll(anotherCacheReader.readMembership(systemRoot.getTree(userPath), cacheLoader));
        assertEquals(3, logCustomizer.getLogs().size());
        //Assert that the cache was used
        assertEquals("Cached membership property '" + newCachePropertyName +"' at " + userPath , logCustomizer.getLogs().get(2));
        assertEquals(1, groupPrincipal.size());

        assertTrue(systemRoot.getTree(userPath).hasChild(REP_CACHE));
        Tree cacheNode = systemRoot.getTree(userPath).getChild(REP_CACHE);
        assertTrue(cacheNode.hasProperty(MembershipCacheConstants.REP_GROUP_PRINCIPAL_NAMES));
        assertTrue(cacheNode.hasProperty(newCachePropertyName));
        assertEquals(cacheNode.getProperty(MembershipCacheConstants.REP_GROUP_PRINCIPAL_NAMES).getValue(Type.STRINGS),
                cacheNode.getProperty(newCachePropertyName).getValue(Type.STRINGS));

    }

    // -------------------------- Helper methods --------------------------------

    @NotNull Tree getTree(@NotNull String id, @NotNull Root root) throws Exception {
        return root.getTree(getUserManager(root).getAuthorizable(id).getPath());
    }

    private @NotNull Principal getPrincipalByName(@NotNull Root root, String s) {
        try {
            return getUserManager(root).getAuthorizable(s).getPrincipal();
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }
}