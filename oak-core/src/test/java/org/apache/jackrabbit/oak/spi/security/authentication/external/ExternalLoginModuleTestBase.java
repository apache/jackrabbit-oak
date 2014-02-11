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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;

/**
 * ExternalLoginModuleTest...
 */
public abstract class ExternalLoginModuleTestBase extends AbstractSecurityTest {

    protected final HashMap<String, Object> options = new HashMap<String, Object>();

    private Set<String> ids = new HashSet<String>();

    private Registration testIdpReg;

    private Registration syncHandlerReg;

    protected Whiteboard whiteboard;

    protected ExternalIdentityProvider idp;

    @Before
    public void before() throws Exception {
        super.before();
        UserManager userManager = getUserManager(root);
        Iterator<Authorizable> iter = userManager.findAuthorizables("jcr:primaryType", null);
        while (iter.hasNext()) {
            ids.add(iter.next().getID());
        }
        idp = createIDP();

        whiteboard = getSecurityProvider().getConfiguration(Whiteboard.class);
        testIdpReg = whiteboard.register(ExternalIdentityProvider.class, idp, Collections.<String, Object>emptyMap());

        options.put(ExternalLoginModule.PARAM_SYNC_HANDLER_NAME, "default");
        options.put(ExternalLoginModule.PARAM_IDP_NAME, idp.getName());

        // set default sync config
        setSyncConfig(new DefaultSyncConfig());
    }

    @After
    public void after() throws Exception {
        if (testIdpReg != null) {
            testIdpReg.unregister();
            testIdpReg = null;
        }
        idp = null;
        setSyncConfig(null);

        try {
            UserManager userManager = getUserManager(root);
            Iterator<Authorizable> iter = userManager.findAuthorizables("jcr:primaryType", null);
            while (iter.hasNext()) {
                ids.remove(iter.next().getID());
            }
            for (String id : ids) {
                Authorizable a = userManager.getAuthorizable(id);
                if (a != null) {
                    a.remove();
                }
            }
            root.commit();
        } finally {
            root.refresh();
            super.after();
        }
    }

    protected abstract ExternalIdentityProvider createIDP();

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