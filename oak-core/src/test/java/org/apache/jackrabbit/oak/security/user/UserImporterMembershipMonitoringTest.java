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

import org.apache.jackrabbit.oak.security.user.monitor.UserMonitor;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.After;

import java.lang.reflect.Field;

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class UserImporterMembershipMonitoringTest extends UserImporterMembershipBesteffortTest {

    private UserMonitor userMonitor;

    @After
    @Override
    public void after() throws Exception {
        try {
            if (groupTree.hasProperty(REP_MEMBERS)) {
                verify(userMonitor, atLeastOnce()).doneUpdateMembers(anyLong(), anyLong(), anyLong(), anyBoolean());
                verify(userMonitor, atMost(2)).doneUpdateMembers(anyLong(), anyLong(), anyLong(), anyBoolean());
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected SecurityProvider initSecurityProvider() {
        SecurityProvider sp = super.initSecurityProvider();
        StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
        UserConfigurationImpl uc = (UserConfigurationImpl) sp.getConfiguration(UserConfiguration.class);
        for (Monitor monitor : uc.getMonitors(statisticsProvider)) {
            if (monitor instanceof UserMonitor) {
                userMonitor = spy((UserMonitor) monitor);

                // register the monitor with the whiteboard to have it accessible in the UserImporterr
                whiteboard.register(monitor.getMonitorClass(), userMonitor, monitor.getMonitorProperties());

                // replace the monitor in the UserConfiguration with the spy
                replaceUserMonitor(uc, userMonitor);
            }
        }
        return sp;
    }

    private static void replaceUserMonitor(@NotNull UserConfigurationImpl uc, @NotNull UserMonitor monitor) {
        try {
            Field f = UserConfigurationImpl.class.getDeclaredField("monitor");
            f.setAccessible(true);
            f.set(uc, monitor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    boolean init(boolean createAction, Class<?>... extraInterfaces) throws Exception {
        boolean b = super.init(createAction, extraInterfaces);

        // verify that the usermonitor has been properly initialized and replace it with the spied instance
        Field f = UserImporter.class.getDeclaredField("userMonitor");
        f.setAccessible(true);
        assertSame(userMonitor, f.get(importer));

        return b;
    }
}