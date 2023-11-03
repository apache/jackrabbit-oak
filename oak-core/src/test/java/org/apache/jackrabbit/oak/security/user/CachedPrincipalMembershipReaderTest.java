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
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.security.principal.AbstractPrincipalProviderTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import javax.jcr.RepositoryException;
import javax.security.auth.Subject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachedPrincipalMembershipReaderTest extends AbstractPrincipalProviderTest {

    static final int NUM_THREADS = 100;
    private Root systemRoot;
    private ContentSession systemSession;
    private UserConfiguration config;
    private String userPath;
    private Root mockedRoot;
    private Tree mockedUser;
    private Logger mockedLogger;
    private PrincipalMembershipReader.GroupPrincipalFactory mockedGroupPrincipalFactory;
    private MembershipProvider mockedMembershipProvider;
    
    @Override
    public void before() throws Exception {
        super.before();

        systemSession = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null));
        systemRoot = systemSession.getLatestRoot();
        userPath = getTestUser().getPath();
        
        config = getUserConfiguration();
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
    protected @NotNull PrincipalProvider createPrincipalProvider() {
        return new UserPrincipalProvider(root, getUserConfiguration(), namePathMapper);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(
                        UserPrincipalProvider.PARAM_CACHE_EXPIRATION, 50000,
                        UserPrincipalProvider.PARAM_CACHE_MAX_STALE, 10000
                )
        );
    }

    private static void setFinalStaticField(@NotNull Class<?> clazz, @NotNull String fieldName, @NotNull Object value)
            throws ReflectiveOperationException {

        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);

        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, value);
    }
    
    private PrincipalMembershipReader.GroupPrincipalFactory createFactory() throws Exception {
        UserPrincipalProvider userPrincipalProvider = (UserPrincipalProvider) createPrincipalProvider();
        Method m = UserPrincipalProvider.class.getDeclaredMethod("createGroupPrincipalFactory");
        m.setAccessible(true);
        return (PrincipalMembershipReader.GroupPrincipalFactory) m.invoke(userPrincipalProvider);
    }

    /**
     * This test checks that 'readMembership' works for objects that are not users
     * @throws RepositoryException
     */
    @Test
    public void testCacheGroupMembershipGetMemberNotUser() throws Exception {
        MembershipProvider membershipProvider = new MembershipProvider(root, getSecurityConfigParameters());

        CachedPrincipalMembershipReader cachedGroupMembershipReader = new CachedPrincipalMembershipReader(membershipProvider, createFactory(), config, root);
        HashSet<Principal> groupPrincipals = new HashSet<>();
        cachedGroupMembershipReader.readMembership(systemRoot.getTree(testGroup2.getPath()), groupPrincipals);
        assertEquals(1, groupPrincipals.size());
    }

    /**
     * This test checks that cache is properly read and written
     * @throws Exception
     */
    @Test
    public void testCacheGroupMembershipGetMember() throws Exception {
        MembershipProvider membershipProvider = new MembershipProvider(root, getSecurityConfigParameters());

        CachedPrincipalMembershipReader cachedGroupMembershipReader = new CachedPrincipalMembershipReader(membershipProvider, createFactory(), config, root);

        Logger log = Mockito.mock(Logger.class);
        setFinalStaticField(cachedGroupMembershipReader.getClass(), "LOG", log);

        Set<Principal> groupPrincipal = new HashSet<>();
        cachedGroupMembershipReader.readMembership(systemRoot.getTree(userPath), groupPrincipal);

        //Assert that the first time the cache was created
        verify(log, times(1)).debug("Create new group membership cache at {}", userPath);
        assertEquals(1, groupPrincipal.size());

        cachedGroupMembershipReader.readMembership(systemRoot.getTree(userPath), groupPrincipal);
        //Assert that the cache was used
        verify(log, times(1)).debug("Reading group membership from cache for {}", userPath);
        assertEquals(1, groupPrincipal.size());

    }

    /**
     * This test checks that stale value is provided during a long commit.
     * Cache is expired, Commit is 3 second longs and NUM_THREADS threads execute getMembership
     * @throws Exception
     */
    @Test
    public void testCacheGroupMembershipGetMemberServeStale() throws Exception {

        initMocks();

        // Test getMembership from multiple threads on expired cache and verify that:
        // - only one thread updated the cache
        // - the stale value was provided

        //Mock UserConfiguration
        UserConfiguration mockedUserConfiguration = mock(UserConfiguration.class);
        @NotNull ConfigurationParameters configurationParameters = ConfigurationParameters.of(
                UserPrincipalProvider.PARAM_CACHE_EXPIRATION, 50000,
                UserPrincipalProvider.PARAM_CACHE_MAX_STALE, 10000
        );
        when(mockedUserConfiguration.getParameters()).thenReturn(configurationParameters);
        testStaleCache(mockedUserConfiguration);
        verify(mockedRoot, times(1)).commit(CacheValidatorProvider.asCommitAttributes());
        verify(mockedLogger, times(NUM_THREADS - 1)).debug("Another thread is updating the cache, returning a stale cache.");
    }
    /**
     * This test checks that stale value is not provided during a long commit when UserPrincipalProvider.PARAM_CACHE_MAX_STALE is 0.
     * Cache is expired, Commit is 3 second longs and NUM_THREADS threads execute getMembership
     * @throws Exception
     */
    @Test
    public void testCacheGroupMembershipGetMemberStaleNotAllowed() throws Exception {

        initMocks();

        // Test getMembership from multiple threads on expired cache and verify that:
        // - only one thread updated the cache
        // - others threads didn't get values since it was not allowed by configuration
        //Mock UserConfiguration
        UserConfiguration mockedUserConfiguration = mock(UserConfiguration.class);
        ConfigurationParameters configurationParameters = ConfigurationParameters.of(
            UserPrincipalProvider.PARAM_CACHE_EXPIRATION, 50000,
            UserPrincipalProvider.PARAM_CACHE_MAX_STALE, 0
        );
        when(mockedUserConfiguration.getParameters()).thenReturn(configurationParameters);

        testStaleCache(mockedUserConfiguration);
        verify(mockedRoot,times(1)).commit(CacheValidatorProvider.asCommitAttributes());
        verify(mockedLogger, times(NUM_THREADS - 1)).debug("Another thread is updating the cache and this thread is not allowed to serve a stale cache; reading from persistence without caching.");
    }

    private void testStaleCache(UserConfiguration mockedUserConfiguration) throws InterruptedException {
        Thread[] getMembershipThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < getMembershipThreads.length; i++) {
            getMembershipThreads[i] = new Thread(() -> {
                Set<Principal> groupPrincipals = new HashSet<>();
                CachedPrincipalMembershipReader cachedGroupMembershipReader = new CachedPrincipalMembershipReader(mockedMembershipProvider, mockedGroupPrincipalFactory, mockedUserConfiguration, mockedRoot);
                try {
                    setFinalStaticField(cachedGroupMembershipReader.getClass(),"LOG", mockedLogger);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                cachedGroupMembershipReader.readMembership(mockedUser, groupPrincipals);
                assertEquals(groupPrincipals.size(),1);
            });
            getMembershipThreads[i].start();
        }
        for (Thread getMembershipThread : getMembershipThreads) {
            getMembershipThread.join();
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

        //Mock logger
        mockedLogger = mock(Logger.class);
    }

}