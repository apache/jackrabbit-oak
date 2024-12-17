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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModuleFactory;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalPrincipalConfiguration;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import javax.jcr.RepositoryException;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Abstract base test for external-authentication tests.
 */
public abstract class AbstractExternalAuthTest extends AbstractSecurityTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    protected static final String USER_ID = TestIdentityProvider.ID_TEST_USER;
    protected static final String TEST_CONSTANT_PROPERTY_NAME = "profile/constantProperty";
    protected static final String TEST_CONSTANT_PROPERTY_VALUE = "constant-value";

    protected ExternalIdentityProvider idp;
    protected DefaultSyncConfig syncConfig;
    protected ExternalPrincipalConfiguration externalPrincipalConfiguration = new ExternalPrincipalConfiguration();

    private Set<String> ids;

    private ContentSession systemSession;
    private Root systemRoot;

    @Before
    public void before() throws Exception {
        super.before();

        getTestUser();
        ids = CollectionUtils.toSet(getAllAuthorizableIds(getUserManager(root)));

        idp = createIDP();
        syncConfig = createSyncConfig();
    }

    @After
    public void after() throws Exception {
        try {
            destroyIDP();
            idp = null;

            // discard any pending changes
            Root r = (systemRoot != null) ? systemRoot : root;
            r.refresh();

            UserManager userManager = getUserManager(r);
            Iterator<String> iter = getAllAuthorizableIds(userManager);
            while (iter.hasNext()) {
                String id = iter.next();
                if (!ids.remove(id)) {
                    Authorizable a = userManager.getAuthorizable(id);
                    if (a != null) {
                        a.remove();
                    }
                }
            }
            r.commit();
        } finally {
            root.refresh();
            if (systemRoot != null) {
                systemRoot.refresh();
            }
            if (systemSession != null) {
                systemSession.close();
            }
            super.after();
        }
    }

    protected static void assertException(@NotNull CommitFailedException e, @NotNull String expectedType, int expectedCode) throws CommitFailedException {
        assertEquals(expectedType, e.getType());
        assertEquals(expectedCode, e.getCode());
        throw e;
    }

    @NotNull
    private static Iterator<String> getAllAuthorizableIds(@NotNull UserManager userManager) throws Exception {
        Iterator<Authorizable> iter = userManager.findAuthorizables("jcr:primaryType", null);
        return Iterators.filter(Iterators.transform(iter, input -> {
            try {
                if (input != null) {
                    return input.getID();
                }
            } catch (RepositoryException e) {
                // failed to retrieve ID
            }
            return null;
        }), Objects::nonNull);
    }

    @Override
    @NotNull
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = TestSecurityProvider.newTestSecurityProvider(getSecurityConfigParameters(), externalPrincipalConfiguration);

            // register PrincipalConfiguration with OSGi context
            context.registerInjectActivateService(externalPrincipalConfiguration, getExternalPrincipalConfiguration());
        }
        return securityProvider;
    }
    
    @NotNull
    protected Map<String, Object> getExternalPrincipalConfiguration() {
        return Collections.emptyMap();
    }

    @NotNull
    protected ExternalIdentityProvider createIDP() {
        return new TestIdentityProvider();
    }

    protected void destroyIDP() {
        // nothing to do
    }

    protected void addIDPUser(String id) {
        ((TestIdentityProvider) idp).addUser(new TestIdentityProvider.TestUser(id, idp.getName()));
    }

    @NotNull
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig syncConfig = new DefaultSyncConfig();
        Map<String, String> mapping = new HashMap<>();
        mapping.put("name", "name");
        mapping.put("email", "email");
        mapping.put("profile/name", "profile/name");
        mapping.put("profile/age", "profile/age");
        mapping.put(TEST_CONSTANT_PROPERTY_NAME, "\"" + TEST_CONSTANT_PROPERTY_VALUE + "\"");
        syncConfig.user().setPropertyMapping(mapping);
        syncConfig.user().setMembershipNestingDepth(1);
        return syncConfig;
    }

    protected DefaultSyncHandler registerSyncHandler(@NotNull Map<String, Object> syncConfigMap, @NotNull String idpName) {
        context.registerService(SyncHandlerMapping.class, mock(ExternalLoginModuleFactory.class), Map.of(
                SyncHandlerMapping.PARAM_IDP_NAME, idpName,
                SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME, syncConfigMap.get(DefaultSyncConfigImpl.PARAM_NAME)
        ));
        return (DefaultSyncHandler) context.registerService(SyncHandler.class, new DefaultSyncHandler(), syncConfigMap);
    }

    protected @NotNull Map<String, Object> syncConfigAsMap() {
        Map<String, Object> builder = Map.of(
            DefaultSyncConfigImpl.PARAM_NAME, syncConfig.getName(),
            DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, syncConfig.user().getDynamicMembership(),
            DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH, syncConfig.user().getMembershipNestingDepth(),
            DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP, syncConfig.user().getAutoMembership().toArray(new String[0]),
            DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP, syncConfig.group().getAutoMembership().toArray(new String[0]),
            DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS, syncConfig.group().getDynamicGroups());
        return Collections.unmodifiableMap(builder);
    }

    @NotNull
    protected Root getSystemRoot() throws Exception {
        if (systemRoot == null) {
            systemSession = Java23Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null));
            systemRoot = systemSession.getLatestRoot();
        }
        return systemRoot;
    }

    protected static void waitUntilExpired(@NotNull User user, @NotNull Root root, long expTime) throws RepositoryException {
        Tree t = root.getTree(user.getPath());
        PropertyState ps = t.getProperty(ExternalIdentityConstants.REP_LAST_SYNCED);
        if (ps == null || ps.count() == 0) {
            return;
        }

        long lastSynced = ps.getValue(Type.LONG);
        long now = Calendar.getInstance().getTimeInMillis();
        while (now - lastSynced <= expTime) {
            now = Calendar.getInstance().getTimeInMillis();
        }
    }

    protected static Set<ExternalIdentityRef> getExpectedSyncedGroupRefs(long membershipNestingDepth, @NotNull ExternalIdentityProvider idp, @NotNull ExternalIdentity extId) throws Exception {
        if (membershipNestingDepth <= 0) {
            return Collections.emptySet();
        }

        Set<ExternalIdentityRef> groupRefs = new HashSet<>();
        getExpectedSyncedGroupRefs(membershipNestingDepth, idp, extId, groupRefs);
        return groupRefs;
    }

    protected static Set<String> getExpectedSyncedGroupIds(long membershipNestingDepth, @NotNull ExternalIdentityProvider idp, @NotNull ExternalIdentity extId) throws Exception {
        Set<ExternalIdentityRef> groupRefs = getExpectedSyncedGroupRefs(membershipNestingDepth, idp, extId);
        return groupRefs.stream().map(ExternalIdentityRef::getId).collect(Collectors.toSet());
    }
    
    private static void getExpectedSyncedGroupRefs(long membershipNestingDepth, @NotNull ExternalIdentityProvider idp, 
                                                  @NotNull ExternalIdentity extId, @NotNull Set<ExternalIdentityRef> groupRefs) throws Exception {
        extId.getDeclaredGroups().forEach(groupRefs::add);
        if (membershipNestingDepth > 1) {
            for (ExternalIdentityRef ref : extId.getDeclaredGroups()) {
                ExternalIdentity id = idp.getIdentity(ref);
                assertNotNull(id);
                getExpectedSyncedGroupRefs(membershipNestingDepth-1, idp, id, groupRefs);
            }
        }
    }
}
