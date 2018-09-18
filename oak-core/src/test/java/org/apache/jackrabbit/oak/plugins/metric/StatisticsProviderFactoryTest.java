/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.metric;

import java.lang.management.ManagementFactory;
import java.util.Collections;

import javax.management.MBeanServer;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StatisticsProviderFactoryTest {

    @Rule
    public final OsgiContext context = new OsgiContext();
    private StatisticsProviderFactory service = new StatisticsProviderFactory();

    @After
    public void registerMBeanServer() {
        context.registerService(MBeanServer.class, ManagementFactory.getPlatformMBeanServer());
    }

    @Test
    public void autoMode() throws Exception {
        MockOsgi.activate(service, context.bundleContext(), Collections.<String, Object>emptyMap());
        assertTrue(context.getService(StatisticsProvider.class) instanceof MetricStatisticsProvider);
        assertNotNull(context.getService(MetricRegistry.class));

        MockOsgi.deactivate(service, context.bundleContext());
        assertNull(context.getService(StatisticsProvider.class));
    }

    @Test
    public void noneMode() throws Exception {
        MockOsgi.activate(service, context.bundleContext(), ImmutableMap.<String, Object>of("providerType", "NONE"));
        assertNull(context.getService(StatisticsProvider.class));
    }

    @Test
    public void defaultMode() throws Exception {
        MockOsgi.activate(service, context.bundleContext(), ImmutableMap.<String, Object>of("providerType", "DEFAULT"));
        assertTrue(context.getService(StatisticsProvider.class) instanceof DefaultStatisticsProvider);
    }
}
