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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.service.cm.ConfigurationAdmin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DocumentNodeStoreServiceConfigurationTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private ConfigurationAdmin configAdmin = context.getService(ConfigurationAdmin.class);

    private TestConfig configuration = new TestConfig(Configuration.PID);

    private TestConfig preset = new TestConfig(Configuration.PRESET_PID);

    @Test
    public void defaultValues() throws Exception {
        Configuration config = createConfiguration();
        assertEquals(DocumentNodeStoreService.DEFAULT_URI, config.mongouri());
        assertEquals(DocumentNodeStoreService.DEFAULT_DB, config.db());
        assertEquals(DocumentNodeStoreService.DEFAULT_SO_KEEP_ALIVE, config.socketKeepAlive());
        assertEquals(DocumentNodeStoreService.DEFAULT_CACHE, config.cache());
        assertEquals(DocumentMK.Builder.DEFAULT_NODE_CACHE_PERCENTAGE, config.nodeCachePercentage());
        assertEquals(DocumentMK.Builder.DEFAULT_PREV_DOC_CACHE_PERCENTAGE, config.prevDocCachePercentage());
        assertEquals(DocumentMK.Builder.DEFAULT_CHILDREN_CACHE_PERCENTAGE, config.childrenCachePercentage());
        assertEquals(DocumentMK.Builder.DEFAULT_DIFF_CACHE_PERCENTAGE, config.diffCachePercentage());
        assertEquals(DocumentMK.Builder.DEFAULT_CACHE_SEGMENT_COUNT, config.cacheSegmentCount());
        assertEquals(DocumentMK.Builder.DEFAULT_CACHE_STACK_MOVE_DISTANCE, config.cacheStackMoveDistance());
        assertEquals(DocumentNodeStoreService.DEFAULT_BLOB_CACHE_SIZE, config.blobCacheSize());
        assertEquals(DocumentNodeStoreService.DEFAULT_PERSISTENT_CACHE, config.persistentCache());
        assertEquals(DocumentNodeStoreService.DEFAULT_JOURNAL_CACHE, config.journalCache());
        assertEquals(DocumentNodeStoreService.DEFAULT_CUSTOM_BLOB_STORE, config.customBlobStore());
        assertEquals(DocumentNodeStoreService.DEFAULT_JOURNAL_GC_INTERVAL_MILLIS, config.journalGCInterval());
        assertEquals(DocumentNodeStoreService.DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS, config.journalGCMaxAge());
        assertEquals(DocumentNodeStoreService.DEFAULT_PREFETCH_EXTERNAL_CHANGES, config.prefetchExternalChanges());
        assertEquals(null, config.role());
        assertEquals(DocumentNodeStoreService.DEFAULT_VER_GC_MAX_AGE, config.versionGcMaxAgeInSecs());
        assertEquals(DocumentNodeStoreService.DEFAULT_VER_GC_EXPRESSION, config.versionGCExpression());
        assertEquals(DocumentNodeStoreService.DEFAULT_RGC_TIME_LIMIT_SECS, config.versionGCTimeLimitInSecs());
        assertEquals(DocumentNodeStoreService.DEFAULT_BLOB_GC_MAX_AGE, config.blobGcMaxAgeInSecs());
        assertEquals(DocumentNodeStoreService.DEFAULT_BLOB_SNAPSHOT_INTERVAL, config.blobTrackSnapshotIntervalInSecs());
        assertEquals(null, config.repository_home());
        assertEquals(DocumentNodeStoreService.DEFAULT_MAX_REPLICATION_LAG, config.maxReplicationLagInSecs());
        assertEquals("MONGO", config.documentStoreType());
        assertEquals(DocumentNodeStoreService.DEFAULT_BUNDLING_DISABLED, config.bundlingDisabled());
        assertEquals(DocumentMK.Builder.DEFAULT_UPDATE_LIMIT, config.updateLimit());
        assertEquals(Arrays.asList("/"), Arrays.asList(config.persistentCacheIncludes()));
    }

    @Test
    public void presetMongoURI() throws Exception {
        String uri = "mongodb://localhost:27017/test";
        addConfigurationEntry(preset, "mongouri", uri);
        Configuration config = createConfiguration();
        assertEquals(uri, config.mongouri());
    }

    @Test
    public void presetSocketKeepAlive() throws Exception {
        boolean keepAlive = !DocumentNodeStoreService.DEFAULT_SO_KEEP_ALIVE;
        addConfigurationEntry(preset, "socketKeepAlive", keepAlive);
        Configuration config = createConfiguration();
        assertEquals(keepAlive, config.socketKeepAlive());
    }

    @Test
    public void presetUpdateLimit() throws Exception {
        int updateLimit = DocumentMK.Builder.DEFAULT_UPDATE_LIMIT / 2;
        addConfigurationEntry(preset, "updateLimit", updateLimit);
        Configuration config = createConfiguration();
        assertEquals(updateLimit, config.updateLimit());
    }

    @Test
    public void presetPersistentCacheIncludes() throws Exception {
        String[] includes = new String[]{"/foo", "/bar"};
        addConfigurationEntry(preset, "persistentCacheIncludes", includes);
        Configuration config = createConfiguration();
        assertTrue(Arrays.equals(includes, config.persistentCacheIncludes()));
    }

    @Test
    public void presetOverridden() throws Exception {
        String db = "test";
        addConfigurationEntry(preset, "db", "foo");
        addConfigurationEntry(configuration, "db", db);
        Configuration config = createConfiguration();
        assertEquals(db, config.db());
    }

    @Test
    public void presetResetToDefault() throws Exception {
        String db = "test";
        addConfigurationEntry(preset, "db", db);
        addConfigurationEntry(configuration, "db", DocumentNodeStoreService.DEFAULT_DB);
        Configuration config = createConfiguration();
        assertEquals(DocumentNodeStoreService.DEFAULT_DB, config.db());
    }

    private Configuration createConfiguration() throws IOException {
        return DocumentNodeStoreServiceConfiguration.create(
                context.componentContext(), configAdmin,
                preset.asConfiguration(),
                configuration.asConfiguration());
    }

    private void addConfigurationEntry(TestConfig config, String key, Object value)
            throws IOException {
        config.put(key, value);
        org.osgi.service.cm.Configuration c = configAdmin.getConfiguration(config.servicePid);
        c.update(new Hashtable(config));
    }

    private static class TestConfig extends HashMap<String, Object> {

        final String servicePid;

        TestConfig(String servicePid) {
            this.servicePid = servicePid;
        }

        Configuration asConfiguration() {
            return (Configuration) Proxy.newProxyInstance(
                    DocumentNodeStoreServiceConfigurationTest.class.getClassLoader(),
                    new Class[]{Configuration.class},
                    this::getProperty);
        }

        private Object getProperty(Object proxy, Method method, Object[] args) {
            Object value = get(method.getName().replaceAll("_", "."));
            if (value == null) {
                value = method.getDefaultValue();
            }
            return value;
        }
    }
}
