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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataIdentifierCreationResult;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataIdentifierFactory;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TypedDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.blob.DataStoreReference.getIdentifierFromReference;
import static org.apache.jackrabbit.oak.spi.blob.DataStoreReference.getReferenceFromIdentifier;

public class CompositeDataStore implements DataStore, SharedDataStore, TypedDataStore, MultiDataStoreAware {

    private static Logger LOG = LoggerFactory.getLogger(CompositeDataStore.class);

    static final String ROLES = "roles";

    private Properties properties = new Properties();
    private Set<DataStore> initialiedDataStores = Sets.newConcurrentHashSet();
    private Set<String> roles = Sets.newConcurrentHashSet();
    private boolean isInitialized = false;

    //@Reference
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
    }

    CompositeDataStore(final Properties properties, final Collection<String> roles) {
        this.properties = properties;
        this.roles = ImmutableSet.copyOf(roles);
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
        synchronized (this) {
            if (!isInitialized) {
                if (Strings.isNullOrEmpty(homeDir)) {
                    throw new IllegalArgumentException("Value required for homeDir");
                }
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

    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        LOG.info("Attempt to retrieve record for identifier \"{}\"", identifier);

        DataRecord result = null;
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator(identifier);
        if (iter.hasNext()) {
            while (iter.hasNext()) {
                DataStore ds = iter.next();
                result = ds.getRecordIfStored(identifier);
                if (null != result) {
                    LOG.info("Matching record found in delegate role \"{}\"", rolesForDelegates.getOrDefault(ds, "<not found>"));
                    break;
                }
            }
        }
        else {
            LOG.info("No mapping for identifier \"{}\", trying all delegates", identifier);
            // Try one more time, this time not limiting delegates by identifier.
            // It may be the case that the identifier is not matched to a delegate yet.
            iter = delegateHandler.getAllDelegatesIterator();
            while (iter.hasNext()) {
                DataStore ds = iter.next();
                result = ds.getRecordIfStored(identifier);
                if (null != result) {
                    delegateHandler.mapIdentifierToDelegate(identifier, ds);
                    LOG.info("Matching record found in delegate role \"{}\"", rolesForDelegates.getOrDefault(ds, "<not found"));
                    break;
                }
            }
        }
        if (null == result) {
            LOG.warn("No record found matching identifier \"{}\"", identifier);
            // Temporary code, follow the lookup path again to trace it
            iter = delegateHandler.getAllDelegatesIterator();
            while (iter.hasNext()) {
                DataStore ds = iter.next();
                result = ds.getRecordIfStored(identifier);
                if (null != result) {
                    break;
                }
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

    private static byte[] referenceKey = null;
    static synchronized byte[] getReferenceKey() throws DataStoreException {
        if (referenceKey == null) {
            referenceKey = new byte[256];
            new SecureRandom().nextBytes(referenceKey);
        }
        return referenceKey;
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        DataIdentifier referenceIdentifier = getIdentifierFromReference(reference);
        if (null != referenceIdentifier) {
            if (reference.equals(getReferenceFromIdentifier(referenceIdentifier, getReferenceKey()))) {
                return getRecordIfStored(referenceIdentifier);
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
        DataIdentifierCreationResult result;
        try {
            result = DataIdentifierFactory.createIdentifier(stream, tmp);
        }
        catch (NoSuchAlgorithmException nsae) {
            throw new DataStoreException("Unable to create identifier from blob stream", nsae);
        }
        catch (IOException ioe) {
            throw new DataStoreException("Unable to save blob stream to temporary file", ioe);
        }

        // NOTE:  There is discussion going on as to whether we should update the last modified
        // time of this object in a read-only delegate if it exists there.  One train of thought
        // is that we should do this, keeping only a single reference instead of creating a copy
        // of the same reference in a writable delegate which is what this current implementation
        // would do.  Garbage collection should work fine in either case, as the references
        // end up not being shared.  It's just less efficient.
        //
        // The problem is knowing for sure that the update to the read-only delegate will only
        // modify the last modified time of the blob.  JCR specifies that addRecord will add
        // the binary or update the last modified time if the binary already exists.  There is
        // potential for a race condition between the time we would check the delegate to see
        // if the binary exists and the time we call addRecord().  If the binary existed but
        // then was deleted before addRecord() was called, that would have the effect of
        // creating the blob again, thus actually modifying the read-only delegate.  There is
        // no API for doing this in the DataStore or related interfaces.
        //
        // For now I'm settling on the idea of having two copies, since I believe it will
        // at least behave consistently and not risk modifying the read-only delegate.
        // -MR

        DataStore selectedDataStore = null;
        Iterator<DataStore> iter = delegateHandler.getWritableDelegatesIterator(result.getIdentifier());
        if (iter.hasNext()) {
            selectedDataStore = iter.next();
        }
        else {
            iter = delegateHandler.getWritableDelegatesIterator();
            if (iter.hasNext()) {
                selectedDataStore = iter.next();
            }
        }

        if (null != selectedDataStore) {
            return writeRecord(selectedDataStore,
                    result.getIdentifier(),
                    result.getTmpFile(),
                    options);
        }
        else {
            LOG.warn("Unable to find a suitable delegate data store for identifier {} in addRecord", result.getIdentifier());
        }
        return null;
    }

    private DataRecord writeRecord(DataStore dataStore, DataIdentifier identifier, File tmpFile, BlobOptions options) throws DataStoreException {
        DataRecord result = null;
        try {
//            if (!(dataStore instanceof CompositeDataStoreAware)) {
//                LOG.warn("Inefficient blob store delegation:  {} using delegate {} which is not an instance of {}",
//                        this.getClass().getSimpleName(),
//                        dataStore.getClass().getSimpleName(),
//                        CompositeDataStoreAware.class.getSimpleName());
//                LOG.warn("Consider rewriting {} to implement {}",
//                        dataStore.getClass().getSimpleName(),
//                        CompositeDataStoreAware.class.getSimpleName());
//                if (dataStore instanceof TypedDataStore) {
//                    result = ((TypedDataStore) dataStore).addRecord(new FileInputStream(tmpFile), options);
//                }
//                else {
                    result = dataStore.addRecord(new FileInputStream(tmpFile));
//                }
//            }
//            else if (dataStore instanceof TypedDataStore) {
//                result = ((CompositeDataStoreAware) dataStore).addRecord(identifier, tmpFile, options);
//            }
//            else {
//                result = ((CompositeDataStoreAware) dataStore).addRecord(identifier, tmpFile);
//            }

            if (null != result) {
                delegateHandler.mapIdentifierToDelegate(identifier, dataStore);
            }
        }
        catch (Exception e) {
            LOG.error("Error adding record");
            throw new DataStoreException("Error adding record", e);
        }
        return result;
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
            // Check to see if this id exists in any delegate.  If it exists but not
            // in a writable delegate, simply do nothing.
            iter = delegateHandler.getAllDelegatesIterator(identifier);
            if (! iter.hasNext()) {
                throw new DataStoreException(String.format("No such record for identifier %s", identifier.toString()));
            }
            return;
        }
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof MultiDataStoreAware) {
                ((MultiDataStoreAware) ds).deleteRecord(identifier);
                delegateHandler.unmapIdentifierFromDelegates(identifier);
            }
        }
    }

    // Metadata records apply to all delegates, including read-only delegates.
    // This is necessary in order to perform GC across shared data stores.
    @Override
    public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
        if (null == stream) {
            throw new IllegalArgumentException("Input stream must not be null");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("A name is required");
        }
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        DataStoreException aggregateException = null;
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                try {
                    ((SharedDataStore) ds).addMetadataRecord(stream, name);
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
    public void addMetadataRecord(File f, String name) throws DataStoreException {
        try {
            addMetadataRecord(new FileInputStream(f), name);
        }
        catch (IOException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
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
        List<DataRecord> records = Lists.newArrayList();
        Iterator<DataStore> iter = delegateHandler.getAllDelegatesIterator();
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                records.addAll(((SharedDataStore) ds).getAllMetadataRecords(prefix));
            }
        }
        return records;
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
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                DataRecord record = ((SharedDataStore) ds).getRecordForId(id);
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