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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.apache.sling.testing.mock.osgi.MockOsgi.activate;
import static org.apache.sling.testing.mock.osgi.MockOsgi.deactivate;
import static org.apache.sling.testing.mock.osgi.MockOsgi.injectServices;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * Tests the registration of the S3DataStore.
 */
public class S3DataStoreServiceTest {

    @Rule
    public OsgiContext context = new OsgiContext();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @BeforeClass
    public static void assumptions() {
        assumeTrue(isS3Configured());
    }

    @Before
    public void setUp() {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
    }


    @Test
    public void testDefaultS3Implementation() throws IOException {
        registerBlobStore();
        assertNotNull(context.getService(AbstractSharedCachingDataStore.class));

        unregisterBlobStore();
    }

    private S3DataStoreService service;

    private void registerBlobStore() throws IOException {
        Map<String, Object> properties = newHashMap();
        properties.putAll(Maps.fromProperties(S3DataStoreUtils.getS3Config()));
        properties.put("repository.home", folder.newFolder().getAbsolutePath());
        service = new S3DataStoreService();
        injectServices(service, context.bundleContext());
        activate(service, context.bundleContext(), properties);
    }

    private void unregisterBlobStore() {
        deactivate(service, context.bundleContext());
    }
}
