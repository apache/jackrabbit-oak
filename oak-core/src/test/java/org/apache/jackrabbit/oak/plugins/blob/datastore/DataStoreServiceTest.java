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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertNotNull;

public class DataStoreServiceTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Test
    public void mbeanRegs() throws Exception{
        Map<String, Object> config = ImmutableMap.<String, Object>of(
                "repository.home", folder.getRoot().getAbsolutePath()
        );

        FileDataStoreService fds = new FileDataStoreService();
        fds.setStatisticsProvider(StatisticsProvider.NOOP);
        MockOsgi.activate(fds, context.bundleContext(), config);

        assertNotNull(context.getService(BlobStoreStatsMBean.class));
        assertNotNull(context.getService(CacheStatsMBean.class));
    }

}
