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

import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that importing the password or impersonators of the existing admin user is monitored (OAK-12495).
 */
public class UserImporterAdminMonitorTest extends UserImporterBaseTest {

    private final Map<String, MeterStats> meters = new HashMap<>();

    private Tree adminTree;

    @Override
    public void before() throws Exception {
        super.before();

        StatisticsProvider statisticsProvider = mock(StatisticsProvider.class);
        when(statisticsProvider.getMeter(anyString(), any(StatsOptions.class))).thenAnswer(
                invocation -> meters.computeIfAbsent(invocation.getArgument(0), name -> mock(MeterStats.class)));
        when(statisticsProvider.getTimer(anyString(), any(StatsOptions.class))).thenReturn(mock(TimerStats.class));
        ((UserConfigurationImpl) getUserConfiguration()).getMonitors(statisticsProvider);

        String adminId = UserUtil.getAdminId(getUserConfiguration().getParameters());
        adminTree = root.getTree(getUserManager(root).getAuthorizable(adminId).getPath());
        init();
    }

    @Test
    public void testImportAdminPassword() throws Exception {
        assertTrue(importer.handlePropInfo(adminTree, createPropInfo(REP_PASSWORD, PasswordUtil.buildPasswordHash("newPw")), mockPropertyDefinition(NT_REP_USER, false)));
        root.commit();

        verify(meters.get("security.user.admin.password.changed"), times(1)).mark();
    }

    @Test
    public void testImportAdminImpersonators() throws Exception {
        assertTrue(importer.handlePropInfo(adminTree, createPropInfo(REP_IMPERSONATORS, testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();
        root.commit();

        verify(meters.get("security.user.admin.impersonators.granted"), times(1)).mark(1);
        verify(meters.get("security.user.admin.impersonators.revoked"), times(1)).mark(0);
        verify(meters.get("security.user.admin.password.changed"), never()).mark();
    }
}
