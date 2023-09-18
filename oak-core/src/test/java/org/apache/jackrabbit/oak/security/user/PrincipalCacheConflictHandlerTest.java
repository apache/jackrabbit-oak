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

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.apache.jackrabbit.oak.security.user.CacheConstants.REP_EXPIRATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrincipalCacheConflictHandlerTest extends AbstractSecurityTest {

    static final String PARAM_CACHE_EXPIRATION = "cacheExpiration";

    @Override
    public void before() throws Exception {
        super.before();

        String groupId = "testGroup" + UUID.randomUUID();
        @NotNull Group testGroup = getUserManager(root).createGroup(groupId);
        testGroup.addMember(getTestUser());

        String groupId2 = "testGroup" + UUID.randomUUID() + "2";
        @NotNull Group testGroup2 = getUserManager(root).createGroup(groupId2);
        testGroup.addMember(testGroup2);

        String groupId3 = "testGroup" + UUID.randomUUID() + "3";
        @NotNull Group testGroup3 = getUserManager(root).createGroup(groupId3);

        root.commit();
    }

    private Tree getCacheTree(Root root) throws Exception {
        return getCacheTree(root, getTestUser().getPath());
    }

    private Tree getCacheTree(Root root, String authorizablePath) {
        return root.getTree(authorizablePath + '/' + PrincipalCacheConflictHandler.REP_CACHE);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(PARAM_CACHE_EXPIRATION, 3600 * 1000)
        );
//        return ConfigurationParameters.of(UserConfiguration.NAME, ConfigurationParameters
//                .of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT,
//               PARAM_CACHE_EXPIRATION, 3600 * 1000));
    }

    @Test
    public void testChangeChangedPropertyLower() throws Exception {

        PrincipalConfiguration pc = getConfig(PrincipalConfiguration.class);

        Root oursRoot = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null)).getLatestRoot();
        Root theirsRoot = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null)).getLatestRoot();

        PrincipalProvider oursPP = pc.getPrincipalProvider(oursRoot, namePathMapper);
        PrincipalProvider theirsPP = pc.getPrincipalProvider(theirsRoot, namePathMapper);

        // set of principals that read from user + membership-provider -> cache being filled
        Set<? extends Principal> ourPrincipals = oursPP.getPrincipals(getTestUser().getID());
        assertTrue(getCacheTree(oursRoot).exists());
        long ourExpiration = getCacheTree(oursRoot).getProperty("rep:expiration").getValue(Type.LONG).longValue();

        Set<? extends Principal> theirPrincipals = theirsPP.getPrincipals(getTestUser().getID());
        assertTrue(getCacheTree(theirsRoot).exists());
        long theirExpiration = getCacheTree(theirsRoot).getProperty("rep:expiration").getValue(Type.LONG).longValue();


        Tree ourCache = getCacheTree(oursRoot);
        ourCache.setProperty(REP_EXPIRATION, 2);
        oursRoot.commit(CacheValidatorProvider.asCommitAttributes());

        root.commit();
        assertEquals(getCacheTree(root).getProperty(REP_EXPIRATION).getValue(Type.LONG).longValue(), theirExpiration);

    }

    @Test
    public void testChangeChangedPropertyHigher() throws Exception {

        PrincipalConfiguration pc = getConfig(PrincipalConfiguration.class);

        Root oursRoot = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null)).getLatestRoot();
        Root theirsRoot = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null)).getLatestRoot();

        PrincipalProvider oursPP = pc.getPrincipalProvider(oursRoot, namePathMapper);
        PrincipalProvider theirsPP = pc.getPrincipalProvider(theirsRoot, namePathMapper);

        // set of principals that read from user + membership-provider -> cache being filled
        Set<? extends Principal> ourPrincipals = oursPP.getPrincipals(getTestUser().getID());
        assertTrue(getCacheTree(oursRoot).exists());
        long ourExpiration = getCacheTree(oursRoot).getProperty("rep:expiration").getValue(Type.LONG).longValue();

        Set<? extends Principal> theirPrincipals = theirsPP.getPrincipals(getTestUser().getID());
        assertTrue(getCacheTree(theirsRoot).exists());
        long theirExpiration = getCacheTree(theirsRoot).getProperty("rep:expiration").getValue(Type.LONG).longValue();


        Tree ourCache = getCacheTree(oursRoot);
        ourCache.setProperty(REP_EXPIRATION, theirExpiration + 1000);
        oursRoot.commit(CacheValidatorProvider.asCommitAttributes());

        root.commit();
        assertEquals(getCacheTree(root).getProperty(REP_EXPIRATION).getValue(Type.LONG).longValue(), theirExpiration + 1000);

    }

    @Test
    public void testChangeChangedPropertyBaseHigher() {
        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        when(ours.getName()).thenReturn(REP_EXPIRATION);
        when(base.getName()).thenReturn(REP_EXPIRATION);
        when(theirs.getName()).thenReturn(REP_EXPIRATION);

        when(ours.getValue(Type.LONG)).thenReturn(1000L);
        when(base.getValue(Type.LONG)).thenReturn(2000L);
        when(theirs.getValue(Type.LONG)).thenReturn(900L);

        PrincipalCacheConflictHandler handler = new PrincipalCacheConflictHandler();
        assertEquals(PrincipalCacheConflictHandler.Resolution.MERGED, handler.changeChangedProperty(parent, ours, theirs, base));

    }

    @Test
    public void testChangeChangedPropertyIgnore() {
        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        PrincipalCacheConflictHandler handler = new PrincipalCacheConflictHandler();
        assertEquals(PrincipalCacheConflictHandler.Resolution.IGNORED, handler.changeChangedProperty(parent, ours, theirs, base));

    }
}