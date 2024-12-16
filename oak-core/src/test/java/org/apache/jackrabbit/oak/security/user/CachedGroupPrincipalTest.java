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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;
import java.util.UUID;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants.PARAM_CACHE_EXPIRATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CachedGroupPrincipalTest extends AbstractSecurityTest {

    private String userId;

    private ContentSession systemSession;
    private Root systemRoot;

    private PrincipalProvider pp;

    protected  String groupId;
    protected Group testGroup;

    @Override
    public void before() throws Exception {
        // because of full text search test #testFindRange
        getQueryEngineSettings().setFailTraversal(false);
        getQueryEngineSettings().setFullTextComparisonWithoutIndex(true);
        super.before();

        groupId = "testGroup" + UUID.randomUUID();
        testGroup = getUserManager(root).createGroup(groupId);
        testGroup.addMember(getTestUser());
        root.commit();

        userId = getTestUser().getID();

        systemSession = getSystemSession();
        systemRoot = systemSession.getLatestRoot();

        // a) force the cache to be created
        pp = new UserPrincipalProvider(systemRoot, getUserConfiguration(), namePathMapper);
        Iterable<? extends Principal> principals = Iterables.filter(pp.getPrincipals(userId), new GroupPredicate()::test);
        for (Principal p : principals) {
            String className = p.getClass().getName();
            assertEquals("org.apache.jackrabbit.oak.security.user.UserPrincipalProvider$GroupPrincipalImpl", className);
        }
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            Authorizable a = getUserManager(root).getAuthorizable(groupId);
            if (a != null) {
                a.remove();
                root.commit();
            }
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
                ConfigurationParameters.of(PARAM_CACHE_EXPIRATION, 3600 * 1000)
        );
    }

    private ContentSession getSystemSession() throws Exception {
        if (systemSession == null) {
            systemSession = Java23Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null));
        }
        return systemSession;
    }

    @Test
    public void testGroupPrincipals() throws Exception {
        Principal testPrincipal = getTestUser().getPrincipal();

        // b) retrieve principals again (this time from the cache)
        // -> verify that they are a different implementation
        Iterable<? extends Principal> principalsAgain = Iterables.filter(pp.getPrincipals(userId), new GroupPredicate()::test);
        for (Principal p : principalsAgain) {
            String className = p.getClass().getName();
            assertEquals("org.apache.jackrabbit.oak.security.user.UserPrincipalProvider$CachedGroupPrincipal", className);

            assertTrue(p instanceof TreeBasedPrincipal);
            assertEquals(testGroup.getPath(), ((TreeBasedPrincipal) p).getPath());
            assertEquals(testGroup.getPath(), ((TreeBasedPrincipal) p).getOakPath());

            GroupPrincipal principalGroup = (GroupPrincipal) p;
            assertTrue(principalGroup.isMember(testPrincipal));

            Enumeration<? extends Principal> members = principalGroup.members();
            assertTrue(members.hasMoreElements());
            assertEquals(testPrincipal, members.nextElement());
            assertFalse(members.hasMoreElements());
        }
    }

    @Test
    public void testCachedPrincipalsGroupRemoved() throws Exception {
        testGroup.remove();
        root.commit();

        systemRoot.refresh();

        // b) retrieve principals again (this time from the cache)
        //    principal for 'testGroup' is no longer backed by an user mgt group
        //    verify that this doesn't lead to runtime exceptions
        Iterable<? extends Principal> principalsAgain = Iterables.filter(pp.getPrincipals(userId), new GroupPredicate()::test);
        for (Principal p : principalsAgain) {
            String className = p.getClass().getName();
            assertEquals("org.apache.jackrabbit.oak.security.user.UserPrincipalProvider$CachedGroupPrincipal", className);

            assertTrue(p instanceof TreeBasedPrincipal);
            try {
                ((TreeBasedPrincipal) p).getPath();
                fail("RepositoryException expected");
            } catch (RepositoryException e) {
                // success
            }

            GroupPrincipal principalGroup = (GroupPrincipal) p;
            assertFalse(principalGroup.isMember(getTestUser().getPrincipal()));

            Enumeration<? extends Principal> members = principalGroup.members();
            assertFalse(members.hasMoreElements());
        }
    }

    @Test
    public void testCachedPrincipalsGroupReplacedByUser() throws Exception {
        String id = testGroup.getID();
        String pName = testGroup.getPrincipal().getName();
        testGroup.remove();
        User u = getUserManager(root).createUser(id, null);
        root.commit();

        systemRoot.refresh();

        // b) retrieve principals again (this time from the cache)
        //    principal for 'testGroup' is no longer backed by an user mgt group
        //    verify that this doesn't lead to runtime exceptions
        Iterable<? extends Principal> principalsAgain = Iterables.filter(pp.getPrincipals(userId), new GroupPredicate()::test);
        for (Principal p : principalsAgain) {
            String className = p.getClass().getName();
            assertEquals("org.apache.jackrabbit.oak.security.user.UserPrincipalProvider$CachedGroupPrincipal", className);
            assertTrue(p instanceof TreeBasedPrincipal);
            try {
                ((TreeBasedPrincipal) p).getPath();
                fail("RepositoryException expected");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test(expected = RepositoryException.class)
    public void testGetOakPathFails() throws Exception {
        PrincipalProvider provider = new UserPrincipalProvider(systemRoot, getUserConfiguration(), new NamePathMapper.Default() {
            @Override
            public String getOakPath(String jcrPath) {
                return null;
            }
        });

        // a) initiate cache
        provider.getPrincipals(userId);

        // b) retrieve principals again (this time from the cache)
        //    principal for 'testGroup' is no longer backed by an user mgt group
        //    verify that this doesn't lead to runtime exceptions
        Iterable<? extends Principal> principalsAgain = Iterables.filter(provider.getPrincipals(userId), new GroupPredicate()::test);
        for (Principal p : principalsAgain) {
            String className = p.getClass().getName();
            assertEquals("org.apache.jackrabbit.oak.security.user.UserPrincipalProvider$CachedGroupPrincipal", className);
            assertTrue(p instanceof TreeBasedPrincipal);
            // accessing oak-path must fail
            ((TreeBasedPrincipal) p).getOakPath();
        }
    }

    //--------------------------------------------------------------------------

    private static final class GroupPredicate implements Predicate<Principal> {
        @Override
        public boolean test(@Nullable Principal input) {
            return (input instanceof GroupPrincipal) && !EveryonePrincipal.getInstance().equals(input);
        }
    }
}