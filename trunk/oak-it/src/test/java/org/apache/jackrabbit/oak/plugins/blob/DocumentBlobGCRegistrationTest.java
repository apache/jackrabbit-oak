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

package org.apache.jackrabbit.oak.plugins.blob;

import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.junit.After;
import org.junit.BeforeClass;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assume.assumeTrue;

/**
 * Tests OSGi registration for {@link BlobGCMBean} in {@link DocumentNodeStoreService}.
 */
public class DocumentBlobGCRegistrationTest extends AbstractBlobGCRegistrationTest {
    private DocumentNodeStoreService service;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(MongoUtils.isAvailable());
    }

    @After
    public void tearDown() throws Exception {
        unregisterNodeStoreService();
        unregisterBlobStore();
        MongoUtils.dropCollections(MongoUtils.DB);
    }

    @Override
    protected void registerNodeStoreService() {
        Map<String, Object> properties = newHashMap();
        properties.put(DocumentNodeStoreService.CUSTOM_BLOB_STORE, true);
        properties.put("repository.home", repoHome);
        properties.put("mongouri", MongoUtils.URL);
        properties.put("db", MongoUtils.DB);
        MockOsgi.setConfigForPid(context.bundleContext(),
                DocumentNodeStoreService.class.getName(), properties);
        context.registerInjectActivateService(new DocumentNodeStoreService.Preset());
        service = context.registerInjectActivateService(new DocumentNodeStoreService());
    }

    @Override
    protected void unregisterNodeStoreService() {
        MockOsgi.deactivate(service, context.bundleContext());
    }
}
