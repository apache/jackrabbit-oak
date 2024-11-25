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

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.framework.ServiceRegistration;

import java.lang.reflect.Field;
import java.util.Hashtable;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.PARAM_DEFAULT_DEPTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class UserConfigurationImplOSGiTest extends AbstractSecurityTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Test
    public void testActivate() {
        UserConfiguration userConfiguration = new UserConfigurationImpl(getSecurityProvider());
        context.registerInjectActivateService(userConfiguration, Map.of(PARAM_DEFAULT_DEPTH, "8"));

        ConfigurationParameters params = userConfiguration.getParameters();
        assertEquals(8, params.getConfigValue(PARAM_DEFAULT_DEPTH, UserConstants.DEFAULT_DEPTH).intValue());
    }
    
    @Test
    public void testDeactivate() {
        UserConfiguration userConfiguration = new UserConfigurationImpl(getSecurityProvider());
        ServiceRegistration sr = context.bundleContext().registerService(new String[] {UserConfiguration.class.getName(), SecurityConfiguration.class.getName()}, 
                userConfiguration, null);
        assertNotNull(context.getService(UserConfiguration.class));
        sr.unregister();
        assertNull(context.getService(UserConfiguration.class));
    }

    @Test
    public void testBlobAccessProviderFromNullWhiteboard() throws Exception {
        SecurityProvider sp = mock(SecurityProvider.class, withSettings().extraInterfaces(WhiteboardAware.class));

        UserConfigurationImpl uc = new UserConfigurationImpl(sp);
        uc.setParameters(ConfigurationParameters.EMPTY);
        uc.setRootProvider(getRootProvider());
        uc.setTreeProvider(getTreeProvider());

        when(sp.getConfiguration(UserConfiguration.class)).thenReturn(uc);

        UserManager um = uc.getUserManager(root, getNamePathMapper());
        assertTrue(um instanceof UserManagerImpl);

        PartialValueFactory vf = ((UserManagerImpl) um).getPartialValueFactory();
        Field f = PartialValueFactory.class.getDeclaredField("blobAccessProvider");
        f.setAccessible(true);
        assertSame(PartialValueFactory.DEFAULT_BLOB_ACCESS_PROVIDER, f.get(vf));
    }
    
    @Test
    public void testBindBlobAccessProvider() throws Exception {
        UserConfigurationImpl uc = new UserConfigurationImpl(getSecurityProvider());
        context.registerInjectActivateService(uc, Map.of(PARAM_DEFAULT_DEPTH, "8"));

        BlobAccessProvider bap = mock(BlobAccessProvider.class);
        uc.getUserManager(root, getNamePathMapper());
        Field f = UserConfigurationImpl.class.getDeclaredField("blobAccessProvider");
        f.setAccessible(true);
        //Validate default service
        assertSame(PartialValueFactory.DEFAULT_BLOB_ACCESS_PROVIDER, f.get(uc));

        ServiceRegistration reg = context.bundleContext().registerService(
                BlobAccessProvider.class.getName(),
                bap, new Hashtable<String, Object>());
        //Validate newly registered service
        assertSame(bap, f.get(uc));

        reg.unregister();
        //Validate default service after unregistering newly registered service
        assertSame(PartialValueFactory.DEFAULT_BLOB_ACCESS_PROVIDER, f.get(uc));
    }
}
