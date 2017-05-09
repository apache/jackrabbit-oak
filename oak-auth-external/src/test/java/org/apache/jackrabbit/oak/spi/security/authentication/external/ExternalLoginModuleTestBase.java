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
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIDPManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncManagerImpl;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;

/**
 * Abstract base test for external-authentication including proper OSGi service
 * registrations required for repository login respecting the {@link ExternalLoginModule}.
 */
public abstract class ExternalLoginModuleTestBase extends AbstractExternalAuthTest {

    private Registration testIdpReg;
    private Registration syncHandlerReg;

    protected final HashMap<String, Object> options = new HashMap<String, Object>();

    protected Whiteboard whiteboard;

    protected SyncManager syncManager;

    protected ExternalIdentityProviderManager idpManager;

    @Before
    public void before() throws Exception {
        super.before();

        testIdpReg = whiteboard.register(ExternalIdentityProvider.class, idp, Collections.<String, Object>emptyMap());

        setSyncConfig(syncConfig);

        options.put(ExternalLoginModule.PARAM_SYNC_HANDLER_NAME, syncConfig.getName());
        options.put(ExternalLoginModule.PARAM_IDP_NAME, idp.getName());
    }

    @After
    public void after() throws Exception {
        try {
            if (testIdpReg != null) {
                testIdpReg.unregister();
                testIdpReg = null;
            }
            setSyncConfig(null);
        } finally {
            super.after();
        }
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