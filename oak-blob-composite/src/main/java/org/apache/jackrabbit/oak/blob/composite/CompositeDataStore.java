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

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TypedDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CompositeDataStore implements DataStore, SharedDataStore, TypedDataStore, MultiDataStoreAware {

    private static Logger LOG = LoggerFactory.getLogger(CompositeDataStore.class);

    static final String ROLES = "roles";

    private Properties properties = new Properties();
    private Set<DataStore> initialiedDataStores = Sets.newConcurrentHashSet();
    private Set<String> roles = Sets.newConcurrentHashSet();
    private boolean isInitialized = false;

    private LoadingCache<DataIdentifier, DataStore> blobIdMap;

    @Reference
    DelegateHandler delegateHandler = new IntelligentDelegateHandler();

    private String path;
    private File rootDirectory;
    private File tmp;

    private Map<DataStore, String> rolesForDelegates = Maps.newConcurrentMap();

    CompositeDataStore(final Properties properties) {
        this.properties = properties;
        if (null == properties) {
            LOG.error("No configuration provided for Composite Data Store");
        }
        roles = getRolesFromConfig(properties);

        blobIdMap = setupIdMapper();
    }

    CompositeDataStore(final Properties properties, final Collection<String> roles) {
        this.properties = properties;
        this.roles = ImmutableSet.copyOf(roles);

        blobIdMap = setupIdMapper();
    }

    private LoadingCache<DataIdentifier, DataStore> setupIdMapper() {
        return CacheBuilder.newBuilder()
                .maximumSize(8192)
                .expireAfterAccess(12, TimeUnit.HOURS)
                .build(
                        new CacheLoader<DataIdentifier, DataStore>() {
                            @Override
                            public DataStore load(DataIdentifier key) throws Exception {
                                return null;
                            }
                        }
                );
    }

    static Set<String> getRolesFromConfig(@Nonnull final Properties properties) {
        Set<String> uniqueRoles = Sets.newConcurrentHashSet();

        String rolestr = properties.getProperty(ROLES, null);
        if (null == rolestr) {
            LOG.error("Configuration element \"roles\" missing from Composite Data Store configuration");
            return uniqueRoles;
        }

        for (String role : rolestr.split(",")) {
            uniqueRoles.add(role.trim());
        }

        return uniqueRoles;
    }

    @Override
    public void init(String homeDir) throws RepositoryException {
        if (Strings.isNullOrEmpty(homeDir)) {
            throw new IllegalArgumentException("Value required for homeDir");
        }

        synchronized (this) {
            if (!isInitialized) {
                isInitialized = true;
                if (path == null) {
                    path = homeDir + "/compositeds";
                }
                path = FilenameUtils.normalizeNoEndSeparator(new File(path).getAbsolutePath());
                LOG.debug("Root path is {}", path);
                this.rootDirectory = new File(path);
                this.tmp = new File(rootDirectory, "tmp");
                if (!this.tmp.exists() && !this.tmp.mkdirs()) {
                    LOG.warn("Unable to create temporary directory");
                }
            }

            Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
            while (iter.hasNext()) {
                DataStore ds = iter.next();
                if (!initialiedDataStores.contains(ds)) {
                    initialiedDataStores.add(ds);
                    String role = rolesForDelegates.get(ds);
                    String delegateHome = FilenameUtils.concat(path, role);
                    ds.init(delegateHome);
                }
            }
        }
    }

    ImmutableSet<String> getRoles() {
        return ImmutableSet.copyOf(roles);
    }

    boolean addDelegate(final DelegateDataStore delegate) {
        String delegateRole = delegate.getRole();
        if (null != delegateRole && roles.contains(delegate.getRole())) {
            LOG.info("Adding delegate with role \"{}\"", delegate.getRole());
            delegateHandler.addDelegateDataStore(delegate);
            rolesForDelegates.put(delegate.getDataStore().getDataStore(), delegateRole);
            return true;
        }
        return false;
    }

    boolean removeDelegate(final DataStoreProvider ds) {
        LOG.info("Removing delegate with role \"{}\"", ds.getRole());
        rolesForDelegates.remove(ds.getDataStore());
        return delegateHandler.removeDelegateDataStore(ds);
    }

    void mapIdToDelegate(final DataIdentifier identifier, final DataStore delegate) {
        blobIdMap.put(identifier, delegate);
    }

    void unmapIdFromDelegate(final DataIdentifier identifier) {
        blobIdMap.invalidate(identifier);
    }

    DataStore getDelegateForId(final DataIdentifier identifier) {
        try {
            return blobIdMap.get(identifier);
        }
        catch (ExecutionException | CacheLoader.InvalidCacheLoadException e) {
            return null;
        }
    }

    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        LOG.info("Attempt to retrieve record for identifier \"{}\"", identifier);

        DataRecord result = null;

        DataStore ds = getDelegateForId(identifier);
        if (null != ds) {
            result = ds.getRecordIfStored(identifier);
        }

        if (null == result) {
            // We get here if:
            // a) The cache doesn't have an entry for identifier (never inserted, expired, etc.)
            // b) The data store no longer has this identifier
            //
            // In each case we want to try again by looking more deeply on our own.
            // If we find it we will put it into the cache for next time.
            try {
                Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator(identifier);
                if (iter.hasNext()) {
                    while (iter.hasNext()) {
                        ds = iter.next();
                        result = ds.getRecordIfStored(identifier);
                        if (null != result) {
                            mapIdToDelegate(identifier, ds);
                            LOG.info("Matching record found in delegate role \"{}\"", rolesForDelegates.getOrDefault(ds, "<not found>"));
                            break;
                        }
                    }
                } else {
                    LOG.info("No mapping for identifier \"{}\", trying all delegates", identifier);
                    // Try one more time, this time not limiting delegates by identifier.
                    // It may be the case that the identifier is not matched to a delegate yet.
                    iter = delegateHandler.getAllDelegatesIterator();
                    while (iter.hasNext()) {
                        ds = iter.next();
                        result = ds.getRecordIfStored(identifier);
                        if (null != result) {
                            mapIdToDelegate(identifier, ds);
                            LOG.info("Matching record found in delegate role \"{}\"", rolesForDelegates.getOrDefault(ds, "<not found"));
                            break;
                        }
                    }
                }
            } catch (DataStoreException e) {
                LOG.error("Error retrieving record [{}]", identifier, e);
                return null;
            }
            if (null == result) {
                LOG.error("Error retrieving record [{}]", identifier);
            }
        }
        return result;
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        DataRecord result = getRecordIfStored(identifier);
        if (null == result) {
            throw new DataStoreException(String.format("No matching record for identifier %s", identifier.toString()));
        }
        return result;
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            DataRecord rec = ds.getRecordFromReference(reference);
            if (null != rec) {
                return rec;
            }
        }
        return null;
    }

    /**
     * Adds a new blob to a CompositeDataStore by invoking addRecord() on an appropriate
     * delegate blob store.  It is the responsibility of the {@link}DelegateHandler to
     * select the delegate that is chosen to which the record will be added.
     *
     * However, some delegate delegate strategies will want to know whether the record
     * already exists in one of the delegates before completing the addition (either to
     * simply return the existing DataRecord from the delegate that already has the
     * record being added, or to move the record to a higher priority delegate, or
     * whatever).  But it is impossible to know whether the record exists without first
     * determining the blob ID for the stream, which can only be determined by reading
     * the entire stream and computing a content hash (content deduplication).
     * This means that the addRecord implementation for composite blob store must:
     * *Read the entire input stream to a temporary file and compute the blob ID.
     * *Ask the delegate delegate strategy to select a writable delegate to add the
     * record to for the provided blob ID.
     * *Delegate the operation to the selected delegate blob store.
     *
     * Prior to calling the delegate, CompositeDataStore will read the incoming stream,
     * save the stream to a temporary file, and compute the appropriate
     * {@link}DataIdentifier for the stream.  If the selected delegate implements
     * {@link}CompositeDataStoreAware, CompositeDataStore will invoke the appropriate
     * addRecord() method on the delegate using the existing {@link}DataIdentifier and
     * the temporary file.  If the delegate does not implement
     * {@link}CompositeDataStoreAware, the delegate will be given an input stream
     * from the temporary file and the delegate will be required to compute the
     * {@link}DataIdentifier from the stream again.
     *
     * @param stream The binary data being added to the CompositeDataStore.
     * @return {@link}DataRecord for the new binary added.  If the binary already existed,
     * this is the {@link}DataRecord for the already existing binary.
     * @throws DataStoreException if the selected delegate data store could not be accessed.
     */
    @Override
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        return this.addRecord(stream, new BlobOptions());
    }

    /**
     * Adds a new blob to a CompositeDataStore by invoking addRecord() on an appropriate
     * delegate blob store, and using the supplied {@link}BlobOptions.
     *
     * @param stream The binary data being added to the CompositeDataStore.
     * @param options Options relevant to choosing the delegate and adding the record.
     * @return {@link}DataRecord for the new binary added.  If the binary already existed,
     * this is the {@link}DataRecord for the already existing binary.
     * @throws DataStoreException if the selected delegate data store could not be accessed.
     */
    @Override
    public DataRecord addRecord(InputStream stream, BlobOptions options) throws DataStoreException {
        if (null == stream) {
            throw new IllegalArgumentException("stream");
        }

        DataStore selectedDataStore = null;
        Iterator<DataStore> iter = delegateHandler.getWritableDelegatesIterator();
        if (iter.hasNext()) {
            selectedDataStore = iter.next();
        }

        if (null != selectedDataStore) {
            return writeRecord(selectedDataStore, stream, options);
        }
        else {
            LOG.warn("Unable to find a suitable delegate data store in addRecord");
        }
        return null;
    }

    private DataRecord writeRecord(DataStore dataStore, InputStream stream, BlobOptions options) throws DataStoreException {
        DataRecord record;
        try {
            if (dataStore instanceof TypedDataStore) {
                record = ((TypedDataStore) dataStore).addRecord(stream, options);
            } else {
                record = dataStore.addRecord(stream);
            }
            if (null != record) {
                mapIdToDelegate(record.getIdentifier(), dataStore);
            }
        }
        catch (Exception e) {
            LOG.error("Error adding record", e);
            throw new DataStoreException("Error adding record", e);
        }
        return record;
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        while (iter.hasNext()) {
            iter.next().updateModifiedDateOnAccess(before);;
        }
    }

    @Override
    public int deleteAllOlderThan(long min) throws DataStoreException {
        // Attempt to delete all records in all writable delegates older than min.
        Iterator<DataStore> iter = delegateHandler.getWritableDelegatesIterator();
        int nDeleted = 0;
        DataStoreException aggregateException = null;
        while (iter.hasNext()) {
            try {
                nDeleted += iter.next().deleteAllOlderThan(min);
            }
            catch (DataStoreException e) {
                if (null == aggregateException) {
                    aggregateException = new DataStoreException(
                            "Aggregate data store exception created in deleteAllOlderThan",
                            e);
                }
                aggregateException.addSuppressed(e);
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
        return nDeleted;
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        // Attempt to iterate through all identifiers in all delegates.
        Iterator<DataIdentifier> result = Iterators.emptyIterator();
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        DataStoreException aggregateException = null;
        while (iter.hasNext()) {
            try {
                result = Iterators.concat(result, iter.next().getAllIdentifiers());
            }
            catch (DataStoreException e) {
                if (null == aggregateException) {
                    aggregateException = new DataStoreException(
                            "Aggregate data store exception created in getAllIdentifiers",
                            e);
                }
                aggregateException.addSuppressed(e);
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
        return result;
    }

    @Override
    public int getMinRecordLength() {
        return delegateHandler.getMinRecordLength();
    }

    @Override
    public void close() throws DataStoreException {
        // Attempt to close all delegates.
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();

        DataStoreException aggregateException = null;

        while (iter.hasNext()) {
            try {
                DataStore ds = iter.next();
                if (null != ds) {
                    ds.close();
                }
            }
            catch (DataStoreException dse) {
                if (null == aggregateException) {
                    aggregateException = new DataStoreException(
                            "Aggregate data store exception created in close",
                            dse);
                }
                aggregateException.addSuppressed(dse);
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
    }

    @Override
    public void clearInUse() {
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        while (iter.hasNext()) {
            iter.next().clearInUse();
        }
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        Iterator<DataStore> iter = delegateHandler.getWritableDelegatesIterator(identifier);
        if (! iter.hasNext()) {
            iter = delegateHandler.getWritableDelegatesIterator();
        }

        boolean deleted = false;
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (null != ds.getRecordIfStored(identifier)) {
                if (ds instanceof MultiDataStoreAware) {
                    ((MultiDataStoreAware) ds).deleteRecord(identifier);
                    deleted = true;
                } else if (ds instanceof DataStoreBlobStore) {
                    try {
                        ((DataStoreBlobStore) ds).deleteChunks(Lists.newArrayList(identifier.toString()), 0L);
                        deleted = true;
                    } catch (Exception e) {
                        throw new DataStoreException(e);
                    }
                }
            }
            if (deleted) {
                unmapIdFromDelegate(identifier);
            }
        }

        if (! deleted) {
            // Check to see if this id exists in any delegate.  If it exists but not
            // in a writable delegate, simply do nothing.
            iter = delegateHandler.getAllDelegatesIterator(identifier);
            if (! iter.hasNext()) {
                iter = delegateHandler.getAllDelegatesIterator();
            }
            if (! iter.hasNext()) {
                throw new DataStoreException(String.format("No such record for identifier %s", identifier.toString()));
            }
            else {
                while (iter.hasNext()) {
                    if (null != iter.next().getRecordIfStored(identifier)) {
                        return;
                    }
                }
            }
            throw new DataStoreException(String.format("No such record for identifier %s", identifier.toString()));
        }
    }

    // Metadata records apply to all delegates, including read-only delegates.
    // This is necessary in order to perform GC across shared data stores.
    @Override
    public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
        if (null == stream) {
            throw new IllegalArgumentException("Input stream must not be null");
        }

        // Create a temporary file and write this stream to the temporary file.
        // Then use the temporary file to call addMetadataRecord on every delegate.
        // Otherwise the stream will be consumed by the first delegate and the
        // write will fail on subsequent delegates.
        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("compositeds-temp", null);
            try (FileOutputStream out = new FileOutputStream(tmpFile)) {
                IOUtils.copy(stream, out);
            }
            addMetadataRecord(tmpFile, name);
        }
        catch (IOException e) {
            throw new DataStoreException(e);
        }
        finally {
            if (null != tmpFile) {
                tmpFile.delete();
            }
        }
    }

    @Override
    public void addMetadataRecord(@Nonnull File f, @Nonnull String name) throws DataStoreException {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("A name is required");
        }

        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        DataStoreException aggregateException = null;
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                try {
                    ((SharedDataStore) ds).addMetadataRecord(f, name);
                }
                catch (DataStoreException dse) {
                    if (null == aggregateException) {
                        aggregateException = new DataStoreException(
                                "Aggregate data store exception created in addMetadataRecord",
                                dse
                        );
                    }
                    aggregateException.addSuppressed(dse);
                }
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        if (null == name) {
            throw new IllegalArgumentException("The wildcard must not be null");
        }

        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                DataRecord result = ((SharedDataStore) ds).getMetadataRecord(name);
                if (null != result) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        Set<DataRecord> records = Sets.newHashSet();
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                records.addAll(((SharedDataStore) ds).getAllMetadataRecords(prefix));
            }
        }
        return Lists.newArrayList(records);
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        boolean result = false;
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                result = result || ((SharedDataStore) ds).deleteMetadataRecord(name);
            }
        }
        return result;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                ((SharedDataStore) ds).deleteAllMetadataRecords(prefix);
            }
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        Iterator<DataRecord> result = Iterators.emptyIterator();
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                result = Iterators.concat(result, ((SharedDataStore) ds).getAllRecords());
            }
        }
        return result;
    }

    @Override
    public DataRecord getRecordForId(@Nonnull DataIdentifier id) throws DataStoreException {
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator(id);
        if (! iter.hasNext()) {
            // Maybe no mapping exists for this identifier yet, so look through all delegates instead.
            iter = delegateHandler.getAllDelegatesIterator();
        }

        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                DataRecord record = null;
                try {
                    record = ((SharedDataStore) ds).getRecordForId(id);
                }
                catch (DataStoreException e) { } // We will throw our own, if not found anywhere
                if (null != record) {
                    return record;
                }
            }
        }
        throw new DataStoreException(
                String.format("No record found for identifier %s", id.toString()));
    }

    @Override
    public Type getType() {
        return Type.SHARED;
    }
}