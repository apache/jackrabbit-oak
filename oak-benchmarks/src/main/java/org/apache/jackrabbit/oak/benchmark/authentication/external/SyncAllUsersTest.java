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

import static org.junit.Assert.assertEquals;

import java.util.List;

import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SyncMBeanImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SynchronizationMBean;
import org.jetbrains.annotations.NotNull;

/**
 * Benchmark for {@link SynchronizationMBean#syncAllUsers(boolean)}
 */
public class SyncAllUsersTest extends AbstractExternalTest {

    private final int expectedUpdates;
    private SynchronizationMBean bean;

    public SyncAllUsersTest(int numberOfUsers, int numberOfGroups, long expTime, boolean dynamicMembership,
            @NotNull List<String> autoMembership) {
        super(numberOfUsers, numberOfGroups, expTime, dynamicMembership, autoMembership);
        if (dynamicMembership) {
            expectedUpdates = numberOfUsers;
        } else {
            expectedUpdates = numberOfUsers + numberOfGroups;
        }
    }

    @Override
    protected Configuration createConfiguration() {
        return ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY);
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();
        bean = new SyncMBeanImpl(getContentRepository(), getSecurityProvider(), syncManager, "default", idpManager,
                idp.getName());
        bean.syncAllExternalUsers();
    }

    @Override
    protected void runTest() throws Exception {
        String[] ops = bean.syncAllUsers(true);
        assertEquals(expectedUpdates, ops.length);
    }
}
