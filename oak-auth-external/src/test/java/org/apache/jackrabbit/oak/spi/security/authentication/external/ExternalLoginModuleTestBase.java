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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIDPManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SynchronizationMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;

/**
 * Abstract base test for external-authentication tests.
 */
public abstract class ExternalLoginModuleTestBase extends AbstractSecurityTest {

    protected static final String USER_ID = TestIdentityProvider.ID_TEST_USER;
    protected static final String TEST_CONSTANT_PROPERTY_NAME = "profile/constantProperty";
    protected static final String TEST_CONSTANT_PROPERTY_VALUE = "constant-value";

    private Set<String> ids;

    private Registration testIdpReg;

    private Registration syncHandlerReg;

    protected final HashMap<String, Object> options = new HashMap<String, Object>();

    protected Whiteboard whiteboard;

    protected ExternalIdentityProvider idp;

    protected SyncManager syncManager;

    protected ExternalIdentityProviderManager idpManager;

    protected DefaultSyncConfig syncConfig;

    @Before
    public void before() throws Exception {
        super.before();
        ids = Sets.newHashSet(getAllAuthorizableIds(getUserManager(root)));
        idp = createIDP();

        testIdpReg = whiteboard.register(ExternalIdentityProvider.class, idp, Collections.<String, Object>emptyMap());

        options.put(ExternalLoginModule.PARAM_SYNC_HANDLER_NAME, "default");
        options.put(ExternalLoginModule.PARAM_IDP_NAME, idp.getName());

        // set default sync config
        syncConfig = new DefaultSyncConfig();
        Map<String, String> mapping = new HashMap<String, String>();
        mapping.put("name", "name");
        mapping.put("email", "email");
        mapping.put("profile/name", "profile/name");
        mapping.put("profile/age", "profile/age");
        mapping.put(TEST_CONSTANT_PROPERTY_NAME, "\"" + TEST_CONSTANT_PROPERTY_VALUE + "\"");
        syncConfig.user().setPropertyMapping(mapping);
        syncConfig.user().setMembershipNestingDepth(1);
        setSyncConfig(syncConfig);
    }

    @After
    public void after() throws Exception {
        if (testIdpReg != null) {
            testIdpReg.unregister();
            testIdpReg = null;
        }
        destroyIDP(idp);
        idp = null;
        setSyncConfig(null);

        try {
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
    protected Oak withEditors(Oak oak) {
        super.withEditors(oak);

        // register non-OSGi managers
        whiteboard = oak.getWhiteboard();
        syncManager = new SyncManagerImpl(whiteboard);
        whiteboard.register(SyncManager.class, syncManager, Collections.emptyMap());
        idpManager = new ExternalIDPManagerImpl(whiteboard);
        whiteboard.register(ExternalIdentityProviderManager.class, idpManager, Collections.emptyMap());

        return oak;
    }

    protected abstract ExternalIdentityProvider createIDP();

    protected abstract void destroyIDP(ExternalIdentityProvider idp);

    protected SynchronizationMBean createMBean() {
        // todo: how to retrieve JCR repository here? maybe we should base the sync mbean on oak directly (=> OAK-4218).
        // JackrabbitRepository repository =  null;
        // return new SyncMBeanImpl(repository, syncManager, "default", idpManager, idp.getName());

        throw new UnsupportedOperationException("creating the mbean is not supported yet.");
    }

    protected void setSyncConfig(DefaultSyncConfig cfg) {
        if (syncHandlerReg != null) {
            syncHandlerReg.unregister();
            syncHandlerReg = null;
        }
        if (cfg != null) {
            syncHandlerReg = whiteboard.register(SyncHandler.class, new DefaultSyncHandler(cfg), Collections.<String, Object>emptyMap());
        }
    }

    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry entry = new AppConfigurationEntry(
                        ExternalLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options);
                return new AppConfigurationEntry[]{entry};
            }
        };
    }
}