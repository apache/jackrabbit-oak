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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;

import javax.security.auth.Subject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class CachedPrincipalMembershipReaderTest extends PrincipalMembershipReaderTest {

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return Lists.newArrayList(
                new Object[] { 10000L, "Cache Max Stale = 10000" },
                new Object[] { UserPrincipalProvider.NO_STALE_CACHE, "Cache Max Stale = 0" });
    }
    
    private static final int NUM_THREADS = 100;
    
    private final long cacheMaxStale;

    private final Logger mockedLogger = mock(Logger.class);

    private ContentSession systemSession;
    private String userPath;
    private Root mockedRoot;
    private Tree mockedUser;
    private PrincipalMembershipReader.GroupPrincipalFactory mockedGroupPrincipalFactory;
    private MembershipProvider mockedMembershipProvider;

    public CachedPrincipalMembershipReaderTest(long cacheMaxStale, @NotNull String name) {
        super();
        this.cacheMaxStale = cacheMaxStale;
    }
    @Override
    public void before() throws Exception {
        super.before();
        userPath = getTestUser().getPath();
        setFinalStaticField(CachedPrincipalMembershipReader.class, "LOG", mockedLogger);
    }

    @Override
    public void after() throws Exception {
        try {
            if (systemSession != null) {
                systemSession.close();
            }
        } finally {
            clearInvocations(mockedLogger);
            super.after();
        }
    }

    @Override
    @NotNull CachedPrincipalMembershipReader createPrincipalMembershipReader(@NotNull Root root) throws Exception {
        return new CachedPrincipalMembershipReader(createMembershipProvider(root), createFactory(root), getUserConfiguration(), root);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(
                        UserPrincipalProvider.PARAM_CACHE_EXPIRATION, 50000,
                        UserPrincipalProvider.PARAM_CACHE_MAX_STALE, cacheMaxStale
                )
        );
    }
    
    private Root getSystemRoot() throws Exception {
        if (systemSession == null) {
            systemSession = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null));
        }
        return systemSession.getLatestRoot();
    }

    private static void setFinalStaticField(@NotNull Class<?> clazz, @NotNull String fieldName, @NotNull Object value) throws Exception {

        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);

        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, value);
    }
    
    @Test
    public void testWritingCacheFailsWithException() throws Exception {
        CachedPrincipalMembershipReader cachedGroupMembershipReader = createPrincipalMembershipReader(root);
        
        Set<Principal> groupPrincipals = new HashSet<>();
        cachedGroupMembershipReader.readMembership(root.getTree(userPath), groupPrincipals);
        
        Set<Principal> expected = Collections.singleton(getUserManager(root).getAuthorizable(groupId).getPrincipal());
        assertEquals(expected, groupPrincipals);

        verify(mockedLogger, times(1)).debug("Attempting to create new membership cache at {}", userPath);
        verify(mockedLogger, times(1)).debug("Failed to cache membership: {}", "OakConstraint0034: Attempt to create or change the system maintained cache.");
        verifyNoMoreInteractions(mockedLogger);
    }

    /**
     * This test checks that 'readMembership' works for objects that are not users
     * @throws Exception
     */
    @Test
    public void testCacheGroupMembershipGetMemberNotUser() throws Exception {
        CachedPrincipalMembershipReader cachedGroupMembershipReader = createPrincipalMembershipReader(root);
        
        Set<Principal> groupPrincipals = new HashSet<>();
        cachedGroupMembershipReader.readMembership(getTree(groupId2, root), groupPrincipals);
        
        Set<Principal> expected = Collections.singleton(getUserManager(root).getAuthorizable(groupId).getPrincipal());
        assertEquals(expected, groupPrincipals);
        
        verifyNoInteractions(mockedLogger);
    }

    /**
     * This test checks that cache is properly read and written
     * @throws Exception
     */
    @Test
    public void testCacheGroupMembershipGetMember() throws Exception {
        Root systemRoot = getSystemRoot();
        CachedPrincipalMembershipReader cachedGroupMembershipReader = createPrincipalMembershipReader(systemRoot);
        
        Set<Principal> groupPrincipal = new HashSet<>();
        cachedGroupMembershipReader.readMembership(systemRoot.getTree(userPath), groupPrincipal);

        //Assert that the first time the cache was created
        verify(mockedLogger, times(1)).debug("Attempting to create new membership cache at {}", userPath);
        assertEquals(1, groupPrincipal.size());

        cachedGroupMembershipReader.readMembership(systemRoot.getTree(userPath), groupPrincipal);
        //Assert that the cache was used
        verify(mockedLogger, times(1)).debug("Reading membership from cache for {}", userPath);
        assertEquals(1, groupPrincipal.size());
    }

    /**
     * This test checks the behavior for an expired cache when the commit is 3 second longs and NUM_THREADS threads execute readMembership:
     * - if UserPrincipalProvider.PARAM_CACHE_MAX_STALE is 0, no stale cache should be provided during a long commit when 
     * - if UserPrincipalProvider.PARAM_CACHE_MAX_STALE is >0, the stale cache must be returned during a long commit.
     * @throws Exception
     */
    @Test
    public void testCacheGroupMembershipGetMemberStaleCache() throws Exception {
        initMocks();

        // Test getMembership from multiple threads on expired cache and verify that:
        // - only one thread updated the cache
        // - the stale value was provided
        Thread[] getMembershipThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < getMembershipThreads.length; i++) {
            getMembershipThreads[i] = new Thread(() -> {
                Set<Principal> groupPrincipals = new HashSet<>();
                CachedPrincipalMembershipReader cachedGroupMembershipReader = new CachedPrincipalMembershipReader(mockedMembershipProvider, mockedGroupPrincipalFactory, getUserConfiguration(), mockedRoot);
                cachedGroupMembershipReader.readMembership(mockedUser, groupPrincipals);
                assertEquals(groupPrincipals.size(),1);
            });
            getMembershipThreads[i].start();
        }
        for (Thread getMembershipThread : getMembershipThreads) {
            getMembershipThread.join();
        }
        
        verify(mockedRoot, times(1)).commit(CacheValidatorProvider.asCommitAttributes());
        if (cacheMaxStale == UserPrincipalProvider.NO_STALE_CACHE) {
            verify(mockedLogger, times(NUM_THREADS - 1)).debug("Another thread is updating the cache and this thread is not allowed to serve a stale cache; reading from persistence without caching.");
        } else {
            verify(mockedLogger, times(NUM_THREADS - 1)).debug("Another thread is updating the cache, returning a stale cache.");
        }
    }

    private void initMocks() throws CommitFailedException {
        mockedRoot = mock(Root.class);
        doAnswer(invocation -> {
            Thread.sleep(3000);
            return null;
        }).when(mockedRoot).commit(CacheValidatorProvider.asCommitAttributes());

        mockedMembershipProvider = mock(MembershipProvider.class);
        mockedGroupPrincipalFactory = mock(PrincipalMembershipReader.GroupPrincipalFactory.class);

        // Mock user Tree
        mockedUser = mock(Tree.class);
        PropertyState userState = mock(PropertyState.class);
        when(userState.getValue(any())).thenReturn(UserConstants.NT_REP_USER);
        when(mockedUser.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(userState);
        when(mockedUser.getPath()).thenReturn(UserConstants.DEFAULT_USER_PATH+"/test");

        // Mock Cache Tree
        Tree mockedPrincipalCache = mock(Tree.class);
        when(mockedPrincipalCache.exists()).thenReturn(true);

        PropertyState propertyStateExpiration = mock(PropertyState.class);
        when(propertyStateExpiration.getValue(Type.LONG)).thenReturn(System.currentTimeMillis());
        when(mockedPrincipalCache.getProperty(CacheConstants.REP_EXPIRATION)).thenReturn(propertyStateExpiration);

        PropertyState propertyStatePrincipalNames = mock(PropertyState.class);
        when(propertyStatePrincipalNames.getValue(Type.STRING)).thenReturn("groupPrincipal");
        when(mockedPrincipalCache.getProperty(CacheConstants.REP_GROUP_PRINCIPAL_NAMES)).thenReturn(propertyStatePrincipalNames);

        when(mockedUser.getChild(CacheConstants.REP_CACHE)).thenReturn(mockedPrincipalCache);

        // Mock Group Tree
        Tree mockedGroupTree = mock(Tree.class);
        PropertyState groupTreeState = mock(PropertyState.class);
        when(groupTreeState.getValue(any())).thenReturn(UserConstants.NT_REP_GROUP);
        when(mockedGroupTree.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(groupTreeState);
        when(mockedGroupTree.getProperty(UserConstants.REP_PRINCIPAL_NAME)).thenReturn(groupTreeState);
        when(mockedMembershipProvider.getMembership(mockedUser, true)).thenAnswer(I -> Arrays.asList(new Tree[]{mockedGroupTree}).iterator());
        Principal groupPrincipal = mock(Principal.class);
        when(groupPrincipal.getName()).thenReturn("groupPrincipal");
        when(mockedGroupPrincipalFactory.create(mockedGroupTree)).thenReturn(groupPrincipal);
    }

}