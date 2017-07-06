/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;

import com.google.common.collect.Maps;
import com.mongodb.DB;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreTestHelper;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class DocumentNodeStoreServiceTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Rule
    public final TemporaryFolder target = new TemporaryFolder(new File("target"));

    private final DocumentNodeStoreService service = new DocumentNodeStoreService();

    private String repoHome;

    @Before
    public void setUp() throws  Exception {
        assumeTrue(MongoUtils.isAvailable());
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        MockOsgi.injectServices(service, context.bundleContext());
        repoHome = target.newFolder().getAbsolutePath();
    }

    @After
    public void tearDown() throws Exception {
        MockOsgi.deactivate(service);
        MongoUtils.dropCollections(MongoUtils.DB);
    }

    @Test
    public void keepAlive() throws Exception {
        Map<String, Object> config = newConfig(repoHome);
        config.put(DocumentNodeStoreService.PROP_SO_KEEP_ALIVE, true);
        MockOsgi.activate(service, context.bundleContext(), config);
        DocumentNodeStore store = context.getService(DocumentNodeStore.class);
        MongoDocumentStore mds = getMongoDocumentStore(store);
        DB db = MongoDocumentStoreTestHelper.getDB(mds);
        assertTrue(db.getMongo().getMongoOptions().isSocketKeepAlive());
    }

    private static MongoDocumentStore getMongoDocumentStore(DocumentNodeStore s) {
        try {
            Field f = s.getClass().getDeclaredField("nonLeaseCheckingStore");
            f.setAccessible(true);
            return (MongoDocumentStore) f.get(s);
        } catch (Exception e) {
            fail(e.getMessage());
            return null;
        }
    }

    private Map<String, Object> newConfig(String repoHome) {
        Map<String, Object> config = Maps.newHashMap();
        config.put("repository.home", repoHome);
        config.put("db", MongoUtils.DB);
        return config;
    }
}
