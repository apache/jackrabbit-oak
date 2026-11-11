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

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that adding a password to an admin user created without password is monitored (OAK-12495).
 */
public class AdminUserMonitorOmitPasswordTest extends AbstractSecurityTest {

    private final Map<String, MeterStats> meters = new HashMap<>();

    @Override
    public void before() throws Exception {
        super.before();

        StatisticsProvider statisticsProvider = mock(StatisticsProvider.class);
        when(statisticsProvider.getMeter(anyString(), any(StatsOptions.class))).thenAnswer(
                invocation -> meters.computeIfAbsent(invocation.getArgument(0), name -> mock(MeterStats.class)));
        when(statisticsProvider.getTimer(anyString(), any(StatsOptions.class))).thenReturn(mock(TimerStats.class));
        ((UserConfigurationImpl) getUserConfiguration()).getMonitors(statisticsProvider);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(UserConstants.PARAM_OMIT_ADMIN_PW, true));
    }

    @NotNull
    @Override
    protected ContentSession createAdminSession(@NotNull ContentRepository repository) {
        try {
            return Java23Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> repository.login(null, null));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAdminPasswordAdded() throws Exception {
        String adminId = UserUtil.getAdminId(getUserConfiguration().getParameters());
        User admin = getUserManager(root).getAuthorizable(adminId, User.class);
        assertFalse(root.getTree(admin.getPath()).hasProperty(UserConstants.REP_PASSWORD));

        admin.changePassword("pw");
        root.commit();
        verify(meters.get("security.user.admin.password.added"), times(1)).mark();
        verify(meters.get("security.user.admin.password.changed"), never()).mark();

        admin.changePassword("pw2");
        root.commit();
        verify(meters.get("security.user.admin.password.added"), times(1)).mark();
        verify(meters.get("security.user.admin.password.changed"), times(1)).mark();
    }
}
