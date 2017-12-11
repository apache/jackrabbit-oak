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

import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalPrincipalConfiguration;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

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
        ids = Sets.newHashSet(getAllAuthorizableIds(getUserManager(root)));

        idp = createIDP();
        syncConfig = createSyncConfig();
    }

    @After
    public void after() throws Exception {
        try {
            destroyIDP();
            idp = null;

            if (systemSession != null) {
                systemSession.close();
            }

            // discard any pending changes
            root.refresh();

            UserManager userManager = getUserManager(root);
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
            root.commit();
        } finally {
            root.refresh();
            super.after();
        }
    }

    private static Iterator<String> getAllAuthorizableIds(@Nonnull UserManager userManager) throws Exception {
        Iterator<Authorizable> iter = userManager.findAuthorizables("jcr:primaryType", null);
        return Iterators.filter(Iterators.transform(iter, new Function<Authorizable, String>() {
            @Nullable
            @Override
            public String apply(Authorizable input) {
                try {
                    if (input != null) {
                        return input.getID();
                    }
                } catch (RepositoryException e) {
                    // failed to retrieve ID
                }
                return null;
            }
        }), Predicates.notNull());
    }

    @Override
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = TestSecurityProvider.newTestSecurityProvider(getSecurityConfigParameters(), externalPrincipalConfiguration);

            // register PrincipalConfiguration with OSGi context
            context.registerInjectActivateService(externalPrincipalConfiguration);
        }
        return securityProvider;
    }

    protected ExternalIdentityProvider createIDP() {
        return new TestIdentityProvider();
    }

    protected void destroyIDP() {
        // nothing to do
    }

    protected void addIDPUser(String id) {
        ((TestIdentityProvider) idp).addUser(new TestIdentityProvider.TestUser(id, idp.getName()));
    }

    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig syncConfig = new DefaultSyncConfig();
        Map<String, String> mapping = new HashMap<String, String>();
        mapping.put("name", "name");
        mapping.put("email", "email");
        mapping.put("profile/name", "profile/name");
        mapping.put("profile/age", "profile/age");
        mapping.put(TEST_CONSTANT_PROPERTY_NAME, "\"" + TEST_CONSTANT_PROPERTY_VALUE + "\"");
        syncConfig.user().setPropertyMapping(mapping);
        syncConfig.user().setMembershipNestingDepth(1);
        return syncConfig;
    }

    protected Root getSystemRoot() throws Exception {
        if (systemRoot == null) {
            systemSession = Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<ContentSession>() {
                @Override
                public ContentSession run() throws LoginException, NoSuchWorkspaceException {
                    return getContentRepository().login(null, null);
                }
            });
            systemRoot = systemSession.getLatestRoot();
        }
        return systemRoot;
    }

    protected static void waitUntilExpired(@Nonnull User user, @Nonnull Root root, long expTime) throws RepositoryException {
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
}