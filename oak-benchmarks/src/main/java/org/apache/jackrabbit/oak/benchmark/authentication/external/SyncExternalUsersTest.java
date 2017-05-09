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

import java.util.List;
import javax.annotation.Nonnull;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SyncMBeanImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SynchronizationMBean;

/**
 * Benchmark for {@link SynchronizationMBean#syncExternalUsers(String[])}
 */
public class SyncExternalUsersTest extends AbstractExternalTest {

    private final int batchSize;
    private SynchronizationMBean bean;

    public SyncExternalUsersTest(int numberOfUsers, int membershipSize, long expTime,
                                 boolean dynamicMembership, @Nonnull List<String> autoMembership,
                                 int batchSize) {
        super(numberOfUsers, membershipSize, expTime, dynamicMembership, autoMembership);
        this.batchSize = batchSize;
    }

    @Override
    protected Configuration createConfiguration() {
        return ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY);
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();
        bean = new SyncMBeanImpl(getContentRepository(), getSecurityProvider(), syncManager, syncConfig.getName(), idpManager, idp.getName());
    }

    @Override
    protected void runTest() throws Exception {
        String[] externalIds = new String[batchSize];
        for (int i = 0; i < batchSize; i++) {
            externalIds[i] = new ExternalIdentityRef(getRandomUserId(), idp.getName()).getString();
        }
        bean.syncExternalUsers(externalIds);
    }
}