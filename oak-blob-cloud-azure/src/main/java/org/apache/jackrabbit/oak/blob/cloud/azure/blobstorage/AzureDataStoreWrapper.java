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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12.AzureDataStoreV12;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TypedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.*;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.data.DataRecord;
import org.apache.jackrabbit.oak.spi.blob.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.*;


/**
 * OSGi component that selects between Azure SDK v8 ({@link AzureDataStore}) and v12
 * ({@link AzureDataStoreV12}) at activation time based on configuration, then registers the
 * chosen implementation under the legacy v8 PID so consumers bound to that PID keep working.
 *
 * <p>Replaces the old dual-service architecture (AzureDataStoreService + AzureDataStoreServiceV12
 * + AzureSDKConditionGate) that caused deadlocks during OSGi service swap on FT toggle.
 */
@Component(
        name = AzureDataStoreWrapper.NAME,
        configurationPid = AzureDataStoreWrapper.NAME,
        configurationPolicy = ConfigurationPolicy.REQUIRE
)
public class AzureDataStoreWrapper extends AbstractDataStoreService {

    private static final Logger log = LoggerFactory.getLogger(AzureDataStoreWrapper.class);

    public static final String NAME = "org.apache.jackrabbit.oak.plugins.blob.datastore.AzureDataStore";

    // Same name for now; kept as separate constants so they can diverge if the sources need different keys later.
    static final String ENV_VAR_V12_ENABLED = "blobstoreAzureV12Enabled";
    static final String OSGI_CONFIG_V12_ENABLED = "blobstoreAzureV12Enabled";
    static final String JVM_PROPERTY_V12_ENABLED = "blob.azure.v12.enabled";
    // Package-private so DelegatingDataStore (inner class) and same-package tests can reach it without reflection.
    AbstractSharedCachingDataStore activeImpl;
    @Reference
    private StatisticsProvider statisticsProvider;
    private ServiceRegistration<AbstractSharedCachingDataStore> delegateReg;

    static ServiceRegistration<AbstractSharedCachingDataStore> registerService(ComponentContext context, AbstractSharedCachingDataStore service) {
        Dictionary<String, Object> delegateProps = new Hashtable<>();
        // Use the v8 PID so consumers bound to "org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore"
        // still receive this service without needing a config change.
        delegateProps.put(Constants.SERVICE_PID, AzureDataStore.class.getName());
        delegateProps.put("oak.datastore.description", new String[]{"type=AzureBlob"});
        return context.getBundleContext().registerService(
                AbstractSharedCachingDataStore.class, service, delegateProps);
    }

    /**
     * Priority: JVM property (test/local override) > env var (fleet-wide container config) > OSGi config (normal production path).
     * Higher-authority sources win so operators can override without touching OSGi config.
     */
    static boolean getUseV12Value(Map<String, Object> config) {
        if (System.getProperty(JVM_PROPERTY_V12_ENABLED) != null) {
            boolean useV12 = Boolean.getBoolean(JVM_PROPERTY_V12_ENABLED);
            log.info("Azure SDK v12 flag: JVM property {}={}", JVM_PROPERTY_V12_ENABLED, useV12);
            return useV12;
        }
        String envVar = System.getenv(ENV_VAR_V12_ENABLED);
        if (StringUtils.isNotBlank(envVar)) {
            boolean useV12 = Boolean.parseBoolean(envVar);
            log.info("Azure SDK v12 flag: environment variable {}={}", ENV_VAR_V12_ENABLED, useV12);
            return useV12;
        }
        if (config.containsKey(OSGI_CONFIG_V12_ENABLED)) {
            boolean useV12 = PropertiesUtil.toBoolean(config.get(OSGI_CONFIG_V12_ENABLED), false);
            log.info("Azure SDK v12 flag: OSGi config {}={}", OSGI_CONFIG_V12_ENABLED, useV12);
            return useV12;
        }
        log.info("Azure SDK v12 flag: not configured, using default (false)");
        return false;
    }

    static AbstractSharedCachingDataStore createV8Store(Properties props) {
        AzureDataStore v8 = new AzureDataStore();
        v8.setProperties(props);
        return v8;
    }

    static AbstractSharedCachingDataStore createV12Store(Properties props) {
        AzureDataStoreV12 v12 = new AzureDataStoreV12();
        v12.setProperties(props);
        return v12;
    }

    private static Properties toProperties(Map<String, Object> config) {
        Properties p = new Properties();
        p.putAll(config);
        return p;
    }

    // -- Helpers ---------------------------------------------------------

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        boolean useV12 = getUseV12Value(config);
        if (useV12) {
            log.info("Starting blob store using Azure SDK v12");
            activeImpl = createV12Store(toProperties(config));
        } else {
            log.info("Starting blob store using Azure SDK v8");
            activeImpl = createV8Store(toProperties(config));
        }
        activeImpl.setStatisticsProvider(getStatisticsProvider());
        // Registers activeImpl separately as AbstractSharedCachingDataStore so consumers
        // bound to that type get the concrete store directly,
        // not just the DataStore view the base class exposes.
        delegateReg = registerService(context, activeImpl);

        return new DelegatingDataStore();
    }

    @Override
    @Deactivate
    protected void deactivate() throws DataStoreException {
        if (delegateReg != null) {
            // Must unregister before super.deactivate() closes the store; otherwise a
            // consumer that unbinds late could receive an already-closed DataStore.
            delegateReg.unregister();
            delegateReg = null;
        }
        super.deactivate();
    }

    @Override
    protected @NotNull StatisticsProvider getStatisticsProvider() {
        return statisticsProvider;
    }

    @Override
    protected void setStatisticsProvider(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
    }

    @Override
    protected String[] getDescription() {
        return new String[]{"type=AzureBlob"};
    }

    // -- Inner DelegatingDataStore (returned from createDataStore) -------

    /**
     * Thin DataStore proxy handed to the base class (AbstractDataStoreService).
     *
     * <p>createDataStore must return a DataStore, but we also need to register activeImpl
     * separately as AbstractSharedCachingDataStore for consumers that bind to that richer type.
     * Returning activeImpl directly would hand ownership to the base class and prevent the
     * separate registration. This delegate keeps the two registrations independent.
     */
    // Must be public: AbstractDataStoreService.createDataStore() reflects into this via
    // PropertiesUtil.populate() (org.apache.jackrabbit.oak.commons, a different package) to set
    // bean properties like cacheSize. A package-private class fails that reflective access even
    // though the setter methods themselves are public.
    public class DelegatingDataStore implements DataStore, ConfigurableDataRecordAccessProvider,
            SharedDataStore, MultiDataStoreAware, TypedDataStore {

        @Override
        public void init(String homeDir) throws DataStoreException {
            activeImpl.init(homeDir);
        }

        @Override
        public DataRecord addRecord(InputStream stream) throws DataStoreException {
            return activeImpl.addRecord(stream);
        }

        @Override
        public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            return activeImpl.getRecord(identifier);
        }

        @Override
        @Nullable
        public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
            return activeImpl.getRecordIfStored(identifier);
        }

        @Override
        @Nullable
        public DataRecord getRecordFromReference(String reference) throws DataStoreException {
            return activeImpl.getRecordFromReference(reference);
        }

        @Override
        public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
            return activeImpl.getAllIdentifiers();
        }

        @Override
        public void updateModifiedDateOnAccess(long before) {
            activeImpl.updateModifiedDateOnAccess(before);
        }

        @Override
        public int deleteAllOlderThan(long min) throws DataStoreException {
            return activeImpl.deleteAllOlderThan(min);
        }

        @Override
        public void clearInUse() {
            activeImpl.clearInUse();
        }

        @Override
        public int getMinRecordLength() {
            return activeImpl.getMinRecordLength();
        }

        @Override
        public void close() throws DataStoreException {
            activeImpl.close();
        }

        // Safe: both AzureDataStore (v8) and AzureDataStoreV12 implement ConfigurableDataRecordAccessProvider.
        private ConfigurableDataRecordAccessProvider provider() {
            return (ConfigurableDataRecordAccessProvider) activeImpl;
        }

        @Override
        public void setDirectUploadURIExpirySeconds(int seconds) {
            provider().setDirectUploadURIExpirySeconds(seconds);
        }

        @Override
        public void setDirectDownloadURIExpirySeconds(int seconds) {
            provider().setDirectDownloadURIExpirySeconds(seconds);
        }

        @Override
        public void setDirectDownloadURICacheSize(int maxSize) {
            provider().setDirectDownloadURICacheSize(maxSize);
        }

        @Override
        public void setBinaryTransferAccelerationEnabled(boolean enabled) {
            provider().setBinaryTransferAccelerationEnabled(enabled);
        }

        @Override
        @Nullable
        public DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes, int maxNumberOfURIs)
                throws IllegalArgumentException, DataRecordUploadException {
            return provider().initiateDataRecordUpload(maxUploadSizeInBytes, maxNumberOfURIs);
        }

        @Override
        @Nullable
        public DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes, int maxNumberOfURIs,
                                                         @NotNull DataRecordUploadOptions options)
                throws IllegalArgumentException, DataRecordUploadException {
            return provider().initiateDataRecordUpload(maxUploadSizeInBytes, maxNumberOfURIs, options);
        }

        @Override
        @NotNull
        public DataRecord completeDataRecordUpload(@NotNull String uploadToken)
                throws IllegalArgumentException, DataRecordUploadException, DataStoreException {
            return provider().completeDataRecordUpload(uploadToken);
        }

        @Override
        @Nullable
        public URI getDownloadURI(@NotNull DataIdentifier identifier,
                                  @NotNull DataRecordDownloadOptions downloadOptions) {
            return provider().getDownloadURI(identifier, downloadOptions);
        }

        // -- SharedDataStore --

        @Override
        public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
            activeImpl.addMetadataRecord(stream, name);
        }

        @Override
        public void addMetadataRecord(File f, String name) throws DataStoreException {
            activeImpl.addMetadataRecord(f, name);
        }

        @Override
        public DataRecord getMetadataRecord(String name) {
            return activeImpl.getMetadataRecord(name);
        }

        @Override
        public boolean metadataRecordExists(String name) {
            return activeImpl.metadataRecordExists(name);
        }

        @Override
        public List<DataRecord> getAllMetadataRecords(String prefix) {
            return activeImpl.getAllMetadataRecords(prefix);
        }

        @Override
        public boolean deleteMetadataRecord(String name) {
            return activeImpl.deleteMetadataRecord(name);
        }

        @Override
        public void deleteAllMetadataRecords(String prefix) {
            activeImpl.deleteAllMetadataRecords(prefix);
        }

        @Override
        public Iterator<DataRecord> getAllRecords() throws DataStoreException {
            return activeImpl.getAllRecords();
        }

        @Override
        public DataRecord getRecordForId(DataIdentifier id) throws DataStoreException {
            return activeImpl.getRecordForId(id);
        }

        @Override
        public SharedDataStore.Type getType() {
            return activeImpl.getType();
        }

        // -- MultiDataStoreAware --

        @Override
        public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
            activeImpl.deleteRecord(identifier);
        }

        // -- TypedDataStore --

        @Override
        public DataRecord addRecord(InputStream input, BlobOptions options) throws DataStoreException {
            return activeImpl.addRecord(input, options);
        }

        // -- Cache-layer setters forwarded so PropertiesUtil.populate() can inject them --

        public void setPath(String path) {
            activeImpl.setPath(path);
        }

        public void setCacheSize(long cacheSize) {
            activeImpl.setCacheSize(cacheSize);
        }

        public void setUploadThreads(int uploadThreads) {
            activeImpl.setUploadThreads(uploadThreads);
        }
    }
}
