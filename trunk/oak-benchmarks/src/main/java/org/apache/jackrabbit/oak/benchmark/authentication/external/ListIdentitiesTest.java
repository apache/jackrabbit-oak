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
package org.apache.jackrabbit.oak.benchmark.authentication.external;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SyncMBeanImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SynchronizationMBean;

/**
 * Benchmark for {@link org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler#listIdentities(UserManager)}
 */
public class ListIdentitiesTest extends AbstractExternalTest {

    private static final List<String> AUTO_IDS = new ArrayList();
    static {
        for (int i = 0; i<100; i++) {
            AUTO_IDS.add("autoGroup"+i);
        }
    }

    public ListIdentitiesTest(int numberOfUsers) {
        super(numberOfUsers, 100, -1, true, AUTO_IDS);
    }

    @Override
    protected Configuration createConfiguration() {
        return ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY);
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();
        SynchronizationMBean bean = new SyncMBeanImpl(getContentRepository(), getSecurityProvider(), syncManager, "default", idpManager, idp.getName());
        bean.syncAllExternalUsers();
    }

    @Override
    protected void runTest() throws Exception {
        JackrabbitSession s = ((JackrabbitSession) systemLogin());
        try {
            UserManager userManager = s.getUserManager();

            Iterator<SyncedIdentity> it = syncHandler.listIdentities(userManager);
            while (it.hasNext()) {
                it.next();
            }
        } finally {
            s.logout();
        }
    }
}