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
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;

import static java.util.List.of;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FGC_BATCH_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FGC_DELAY_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FGC_PROGRESS_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FULL_GC_ENABLED;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_EMBEDDED_VERIFICATION_ENABLED;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FULL_GC_MODE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_THROTTLING_ENABLED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DocumentNodeStoreServiceConfigurationTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final ConfigurationAdmin configAdmin = context.getService(ConfigurationAdmin.class);

    private final TestConfig configuration = new TestConfig(Configuration.PID);

    private final TestConfig preset = new TestConfig(Configuration.PRESET_PID);

    @Test
    public void defaultValues() throws Exception {
        Configuration config = createConfiguration();
        assertEquals(DocumentNodeStoreService.DEFAULT_URI, config.mongouri());
        assertEquals(DocumentNodeStoreService.DEFAULT_DB, config.db());
        assertEquals(DocumentNodeStoreService.DEFAULT_SO_KEEP_ALIVE, config.socketKeepAlive());
        assertEquals(DocumentNodeStoreService.DEFAULT_MONGO_LEASE_SO_TIMEOUT_MILLIS, config.mongoLeaseSocketTimeout());
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
        assertNull(config.role());
        assertEquals(DocumentNodeStoreService.DEFAULT_VER_GC_MAX_AGE, config.versionGcMaxAgeInSecs());
        assertEquals(DocumentNodeStoreService.DEFAULT_VER_GC_EXPRESSION, config.versionGCExpression());
        assertEquals(DocumentNodeStoreService.DEFAULT_RGC_TIME_LIMIT_SECS, config.versionGCTimeLimitInSecs());
        assertEquals(DocumentNodeStoreService.DEFAULT_BLOB_GC_MAX_AGE, config.blobGcMaxAgeInSecs());
        assertEquals(DocumentNodeStoreService.DEFAULT_BLOB_SNAPSHOT_INTERVAL, config.blobTrackSnapshotIntervalInSecs());
        assertNull(config.repository_home());
        assertEquals(DocumentNodeStoreService.DEFAULT_MAX_REPLICATION_LAG, config.maxReplicationLagInSecs());
        assertEquals("MONGO", config.documentStoreType());
        assertEquals(DocumentNodeStoreService.DEFAULT_BUNDLING_DISABLED, config.bundlingDisabled());
        assertEquals(DocumentMK.Builder.DEFAULT_UPDATE_LIMIT, config.updateLimit());
        assertEquals(of("/"), Arrays.asList(config.persistentCacheIncludes()));
        assertEquals(of("/"), of(config.fullGCIncludePaths()));
        assertEquals(of(), of(config.fullGCExcludePaths()));
        assertEquals("STRICT", config.leaseCheckMode());
        assertEquals(DEFAULT_THROTTLING_ENABLED, config.throttlingEnabled());
        assertEquals(DEFAULT_FULL_GC_ENABLED, config.fullGCEnabled());
        assertEquals(DEFAULT_FULL_GC_MODE, config.fullGCMode());
        assertEquals(DEFAULT_FGC_DELAY_FACTOR, config.fullGCDelayFactor(), 0.01);
        assertEquals(DEFAULT_FGC_BATCH_SIZE, config.fullGCBatchSize());
        assertEquals(DEFAULT_FGC_PROGRESS_SIZE, config.fullGCProgressSize());
        assertEquals(DEFAULT_FULL_GC_ENABLED, config.fullGCEnabled());
        assertEquals(DEFAULT_EMBEDDED_VERIFICATION_ENABLED, config.embeddedVerificationEnabled());
        assertEquals(CommitQueue.DEFAULT_SUSPEND_TIMEOUT, config.suspendTimeoutMillis());
    }

    @Test
    public void presetMongoURI() throws Exception {
        String uri = "mongodb://localhost:27017/test";
        addConfigurationEntry(preset, "mongouri", uri);
        Configuration config = createConfiguration();
        assertEquals(uri, config.mongouri());
    }

    @Test
    public void throttleEnabled() throws Exception {
        boolean throttleDocStore = true;
        addConfigurationEntry(preset, "throttlingEnabled", throttleDocStore);
        Configuration config = createConfiguration();
        assertEquals(throttleDocStore, config.throttlingEnabled());
    }

    @Test
    public void fullGCEnabled() throws Exception {
        boolean fullGCDocStore = true;
        addConfigurationEntry(preset, "fullGCEnabled", fullGCDocStore);
        Configuration config = createConfiguration();
        assertEquals(fullGCDocStore, config.fullGCEnabled());
    }

    @Test
    public void fullGCModeValueSet() throws Exception {
        int fullGCModeValue = 2;
        addConfigurationEntry(preset, "fullGCMode", fullGCModeValue);
        Configuration config = createConfiguration();
        assertEquals(fullGCModeValue, config.fullGCMode());
    }

    @Test
    public void fullGCIncludePaths() throws Exception {
        final String[] includesPath = new String[]{"/foo", "/bar"};
        addConfigurationEntry(preset, "fullGCIncludePaths", includesPath);
        Configuration config = createConfiguration();
        assertArrayEquals(includesPath, config.fullGCIncludePaths());
    }

    @Test
    public void fullGCExcludePaths() throws Exception {
        final String[] excludesPath = new String[]{"/foo", "/bar"};
        addConfigurationEntry(preset, "fullGCExcludePaths", excludesPath);
        Configuration config = createConfiguration();
        assertArrayEquals(excludesPath, config.fullGCExcludePaths());
    }

    @Test
    public void embeddedVerificationEnabled() throws Exception {
        boolean embeddedVerificationEnabled = false;
        addConfigurationEntry(preset, "embeddedVerificationEnabled", embeddedVerificationEnabled);
        Configuration config = createConfiguration();
        assertEquals(embeddedVerificationEnabled, config.embeddedVerificationEnabled());
    }

    @Test
    public void fullGCBatchSize() throws Exception {
        int batchSize = 2000;
        addConfigurationEntry(preset, "fullGCBatchSize", batchSize);
        Configuration config = createConfiguration();
        assertEquals(batchSize, config.fullGCBatchSize());
    }

    @Test
    public void fullGCProgressSize() throws Exception {
        int progressSize = 20000;
        addConfigurationEntry(preset, "fullGCProgressSize", progressSize);
        Configuration config = createConfiguration();
        assertEquals(progressSize, config.fullGCProgressSize());
    }

    @Test
    public void fullGCDelayFactor() throws Exception {
        double fullGCDelayFactor = 0.5d;
        addConfigurationEntry(preset, "fullGCDelayFactor", fullGCDelayFactor);
        Configuration config = createConfiguration();
        assertEquals(fullGCDelayFactor, config.fullGCDelayFactor(), 0.01);
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
    public void presetLeaseSocketTimeout() throws Exception {
        int timeout = DocumentNodeStoreService.DEFAULT_MONGO_LEASE_SO_TIMEOUT_MILLIS / 2;
        addConfigurationEntry(preset, "mongoLeaseSocketTimeout", timeout);
        Configuration config = createConfiguration();
        assertEquals(timeout, config.mongoLeaseSocketTimeout());
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

    @Test
    public void overridePersistentCacheIncludes() throws Exception {
        BundleContext bundleContext = Mockito.mock(BundleContext.class);
        Mockito.when(bundleContext.getProperty("oak.documentstore.persistentCacheIncludes")).thenReturn("/foo::/bar");
        ComponentContext componentContext = Mockito.mock(ComponentContext.class);
        Mockito.when(componentContext.getBundleContext()).thenReturn(bundleContext);
        Configuration config = DocumentNodeStoreServiceConfiguration.create(
                componentContext, configAdmin,
                preset.asConfiguration(),
                configuration.asConfiguration());
        assertArrayEquals(new String[]{"/foo", "/bar"}, config.persistentCacheIncludes());
    }

    @Test
    public void overrideFullGCIncludePaths() throws Exception {
        BundleContext bundleContext = Mockito.mock(BundleContext.class);
        Mockito.when(bundleContext.getProperty("oak.documentstore.fullGCIncludePaths")).thenReturn("/foo::/bar");
        ComponentContext componentContext = Mockito.mock(ComponentContext.class);
        Mockito.when(componentContext.getBundleContext()).thenReturn(bundleContext);
        Configuration config = DocumentNodeStoreServiceConfiguration.create(
                componentContext, configAdmin,
                preset.asConfiguration(),
                configuration.asConfiguration());
        assertArrayEquals(new String[]{"/foo", "/bar"}, config.fullGCIncludePaths());
    }

    @Test
    public void overrideFullGCExcludePaths() throws Exception {
        BundleContext bundleContext = Mockito.mock(BundleContext.class);
        Mockito.when(bundleContext.getProperty("oak.documentstore.fullGCExcludePaths")).thenReturn("/foo::/bar");
        ComponentContext componentContext = Mockito.mock(ComponentContext.class);
        Mockito.when(componentContext.getBundleContext()).thenReturn(bundleContext);
        Configuration config = DocumentNodeStoreServiceConfiguration.create(
                componentContext, configAdmin,
                preset.asConfiguration(),
                configuration.asConfiguration());
        assertArrayEquals(new String[]{"/foo", "/bar"}, config.fullGCExcludePaths());
    }

    @Test
    public void recoveryDelayMillis() throws Exception {
        addConfigurationEntry(preset, "recoveryDelayMillis", 0L);
        Configuration config = createConfiguration();
        assertEquals(0L, config.recoveryDelayMillis());

        addConfigurationEntry(preset, "recoveryDelayMillis", -1L);
        config = createConfiguration();
        assertEquals(-1L, config.recoveryDelayMillis());

        addConfigurationEntry(preset, "recoveryDelayMillis", 60000L);
        config = createConfiguration();
        assertEquals(60000L, config.recoveryDelayMillis());
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
