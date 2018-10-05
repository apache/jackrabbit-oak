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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import java.lang.reflect.Field;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.felix.jaas.boot.ProxyLoginModule;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModuleTestBase;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIDPManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModuleFactory;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncManagerImpl;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SynMBeanImplOSGiTest extends ExternalLoginModuleTestBase {

    private ExternalLoginModuleFactory externalLoginModuleFactory;

    @Override
    public void before() throws Exception {
        super.before();

        context.registerService(SyncManager.class, new SyncManagerImpl(whiteboard));
        context.registerService(ExternalIdentityProviderManager.class, new ExternalIDPManagerImpl(whiteboard));

        externalLoginModuleFactory = new ExternalLoginModuleFactory();

        context.registerInjectActivateService(externalLoginModuleFactory);
        assertSyncBeanRegistration(externalLoginModuleFactory, false);
    }

    @Override
    protected Oak withEditors(Oak oak) {
        super.withEditors(oak);
        //Just grab the whiteboard but do not register any manager here
        //This would ensure that LoginModule would only work if the required managers
        //are preset
        whiteboard = oak.getWhiteboard();
        return oak;
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

    @Test
    public void testRegisterContentRepository() throws Exception {
        context.registerService(ContentRepository.class, getContentRepository());
        assertSyncBeanRegistration(externalLoginModuleFactory, false);
    }

    @Test
    public void testRegisterSecurityProvider() throws Exception {
        context.registerService(SecurityProvider.class, getSecurityProvider());
        assertSyncBeanRegistration(externalLoginModuleFactory, false);
    }

    @Test
    public void testContentRepositoryAndSecurityProviderServices() throws Exception {
        context.registerService(ContentRepository.class, getContentRepository());
        context.registerService(SecurityProvider.class, getSecurityProvider());
        assertSyncBeanRegistration(externalLoginModuleFactory, true);
    }

    @Test
    public void testBind() throws Exception {
        externalLoginModuleFactory.bindSecurityProvider(getSecurityProvider());
        assertSyncBeanRegistration(externalLoginModuleFactory, false);

        externalLoginModuleFactory.bindContentRepository(getContentRepository());
        assertSyncBeanRegistration(externalLoginModuleFactory, true);
    }

    @Test
    public void testUnbind() throws Exception {
        externalLoginModuleFactory.bindSecurityProvider(getSecurityProvider());
        externalLoginModuleFactory.bindContentRepository(getContentRepository());

        externalLoginModuleFactory.unbindContentRepository(getContentRepository());
        assertSyncBeanRegistration(externalLoginModuleFactory, false);

        externalLoginModuleFactory.unbindSecurityProvider(getSecurityProvider());
        assertSyncBeanRegistration(externalLoginModuleFactory, false);
    }

    @Test
    public void testDeactivateFactory() throws Exception {
        context.registerService(ContentRepository.class, getContentRepository());
        context.registerService(SecurityProvider.class, getSecurityProvider());

        MockOsgi.deactivate(externalLoginModuleFactory, context.bundleContext());
        assertSyncBeanRegistration(externalLoginModuleFactory, false);
    }

    private static void assertSyncBeanRegistration(ExternalLoginModuleFactory externalLoginModuleFactory, boolean exists) throws Exception {
        Field f = ExternalLoginModuleFactory.class.getDeclaredField("mbeanRegistration");
        f.setAccessible(true);

        Object mbeanRegistration = f.get(externalLoginModuleFactory);
        if (exists) {
            assertNotNull(mbeanRegistration);
        } else {
            assertNull(mbeanRegistration);
        }
    }
}