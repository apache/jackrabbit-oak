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

package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jcr.Repository;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.spi.LoginModule;

import org.apache.felix.jaas.LoginModuleFactory;
import org.apache.felix.jaas.boot.ProxyLoginModule;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIDPManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModuleFactory;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncManagerImpl;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test uses quite a bit of logic from ExternalLoginModuleTestBase
 * As we need to create very specific scenario it would be easier to control
 * the whole scenario
 */
public class ExternalLoginModuleFactoryTest extends AbstractSecurityTest {
    private final static String TEST_CONSTANT_PROPERTY_NAME = "profile/constantProperty";

    private final static String TEST_CONSTANT_PROPERTY_VALUE = "constant-value";

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final HashMap<String, Object> options = new HashMap<String, Object>();

    private final String userId = "testUser";

    private ExternalIdentityProvider idp;

    private DefaultSyncConfig syncConfig;

    private Registration testIdpReg;

    private Registration syncHandlerReg;

    private Whiteboard whiteboard;

    @Override
    protected Oak withEditors(Oak oak) {
        super.withEditors(oak);
        //Just grab the whiteboard but do not register any manager here
        //This would ensure that LoginModule would only work if the required managers
        //are preset
        whiteboard = oak.getWhiteboard();
        return oak;
    }

    @Before
    public void before() throws Exception {
        super.before();

        idp = new TestIdentityProvider();
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
        mapping.put(TEST_CONSTANT_PROPERTY_NAME, "\"constant-value\"");
        syncConfig.user().setPropertyMapping(mapping);
        syncConfig.user().setMembershipNestingDepth(1);
        syncHandlerReg = whiteboard.register(SyncHandler.class, new DefaultSyncHandler(syncConfig), Collections.<String, Object>emptyMap());
    }

    @After
    public void after() throws Exception {
        testIdpReg.unregister();
        syncHandlerReg.unregister();

        try {
            UserManager userManager = getUserManager(root);
            Authorizable a = userManager.getAuthorizable(userId);
            if (a != null) {
                a.remove();
            }
            root.commit();
        } finally {
            root.refresh();
            super.after();
        }
    }

    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry entry = new AppConfigurationEntry(
                        //Use ProxyLoginModule so that factory mode can be used
                        ProxyLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options);
                return new AppConfigurationEntry[]{entry};
            }
        };
    }

    //~-------------------------------------------< tests >

    /**
     * Prepares the OSGi part with required services injected and configures
     * the factory in JAAS options which then delegates to ExternalLoginModuleFactory
     */
    public void setUpJaasFactoryWithInjection() throws Exception{
        context.registerService(Repository.class, EasyMock.createMock(Repository.class));
        context.registerService(SecurityProvider.class, EasyMock.createMock(SecurityProvider.class));
        context.registerService(SyncManager.class, new SyncManagerImpl(whiteboard));
        context.registerService(ExternalIdentityProviderManager.class, new ExternalIDPManagerImpl(whiteboard));

        final LoginModuleFactory lmf = context.registerInjectActivateService(new ExternalLoginModuleFactory());
        options.put(ProxyLoginModule.PROP_LOGIN_MODULE_FACTORY, new ProxyLoginModule.BootLoginModuleFactory() {
            @Override
            public LoginModule createLoginModule() {
                return lmf.createLoginModule();
            }
        });
    }

    @Test
    public void testSyncCreateUser() throws Exception {
        setUpJaasFactoryWithInjection();
        UserManager userManager = getUserManager(root);
        ContentSession cs = null;
        try {
            assertNull(userManager.getAuthorizable(userId));

            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();

            Authorizable a = userManager.getAuthorizable(userId);
            assertNotNull(a);
            ExternalUser user = idp.getUser(userId);
            for (String prop : user.getProperties().keySet()) {
                assertTrue(a.hasProperty(prop));
            }
            assertEquals(TEST_CONSTANT_PROPERTY_VALUE, a.getProperty(TEST_CONSTANT_PROPERTY_NAME)[0].getString());
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

}
