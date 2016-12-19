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

package org.apache.jackrabbit.oak.upgrade.blob;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.jcr.RepositoryException;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.core.data.AbstractDataRecord;
import org.apache.jackrabbit.core.data.AbstractDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A DelegatingDataStore can avoid performing expensive file system access by making
 * use of pre computed data related to files in DataStore.
 * <p>
 * During repository migration actual blob content is not accessed and instead
 * only the blob length and blob references are accessed. DelegatingDataStore can be
 * configured with a mapping file which would be used to determine the length of given
 * blob reference.
 * <p>
 * Mapping file format
 * <pre>{@code
 *     #< length >| < identifier >
 *     4432|dd10bca036f3134352c63e534d4568a3d2ac2fdc
 *     32167|dd10bca036f3134567c63e534d4568a3d2ac2fdc
 * }</pre>
 * <p>
 * The Configuration:
 * <pre>{@code
 *  <DataStore class="org.apache.jackrabbit.oak.upgrade.blob.LengthCachingDataStore">
 *      <param name="mappingFilePath" value="/path/to/mapping/file" />
 *      <param name="delegateClass" value="org.apache.jackrabbit.core.data.FileDataStore" />
 *  </DataStore>
 * }</pre>
 */
public class LengthCachingDataStore extends AbstractDataStore {
    private static final Logger log = LoggerFactory.getLogger(LengthCachingDataStore.class);
    /**
     * Separator used while writing length and identifier to the mapping file
     */
    public static final char SEPARATOR = '|';

    //TODO For now using an in memory map. For very large repositories
    //this might consume lots of memory. For such case we would need to switch to
    //some off heap map
    private Map<String, Long> existingMappings = Collections.emptyMap();
    private Map<String, Long> newMappings = Maps.newConcurrentMap();

    private String mappingFilePath = "datastore-list.txt";
    private String delegateClass;
    private String delegateConfigFilePath;
    private DataStore delegate;
    private boolean readOnly = true;
    private File mappingFile;

    @Override
    public void init(String homeDir) throws RepositoryException {
        initializeDelegate(homeDir);
        initializeMappingData(homeDir);
    }

    @Override
    public DataRecord getRecordIfStored(DataIdentifier dataIdentifier) throws DataStoreException {
        if (existingMappings.containsKey(dataIdentifier.toString())) {
            return new DelegateDataRecord(this, dataIdentifier, existingMappings);
        } else if (newMappings.containsKey(dataIdentifier.toString())) {
            return new DelegateDataRecord(this, dataIdentifier, newMappings);
        }
        DataRecord result = getDelegate().getRecordIfStored(dataIdentifier);
        addNewMapping(result);
        return result;
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        //Override the getRecordFromReference so that reference handling does not
        //trigger a call to FS
        if (reference != null) {
            int colon = reference.indexOf(':');
            if (colon != -1) {
                return getRecordIfStored(new DataIdentifier(reference.substring(0, colon)));
            }
        }
        return null;
    }

    @Override
    public DataRecord addRecord(InputStream inputStream) throws DataStoreException {
        checkIfReadOnly();
        DataRecord result = getDelegate().addRecord(inputStream);
        addNewMapping(result);
        return result;
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {
        checkIfReadOnly();
        getDelegate().updateModifiedDateOnAccess(before);

    }

    @Override
    public int deleteAllOlderThan(long min) throws DataStoreException {
        checkIfReadOnly();
        return getDelegate().deleteAllOlderThan(min);
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return getDelegate().getAllIdentifiers();
    }

    @Override
    public int getMinRecordLength() {
        return getDelegate().getMinRecordLength();
    }

    @Override
    public void close() throws DataStoreException {
        existingMappings.clear();
        saveNewMappingsToFile();

        if (delegate != null) {
            delegate.close();
        }
    }

    @Override
    public void clearInUse() {
        getDelegate().clearInUse();
    }

    File getMappingFile() {
        return mappingFile;
    }

    //~---------------------------------< Setters >

    public void setMappingFilePath(String mappingFilePath) {
        this.mappingFilePath = mappingFilePath;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setDelegateClass(String delegateClass) {
        this.delegateClass = delegateClass;
    }

    public void setDelegateConfigFilePath(String delegateConfigFilePath) {
        this.delegateConfigFilePath = delegateConfigFilePath;
    }

    //~---------------------------------< DelegateDataRecord >

    private class DelegateDataRecord extends AbstractDataRecord {
        private final Map<String, Long> mapping;
        private DataRecord delegateRecord;

        public DelegateDataRecord(AbstractDataStore store, DataIdentifier identifier,
                                  Map<String, Long> recordSizeMapping) {
            super(store, identifier);
            this.mapping = recordSizeMapping;
        }

        @Override
        public long getLength() throws DataStoreException {
            Long size = mapping.get(getIdentifier().toString());
            if (size == null) {
                log.info("No size mapping found for {}. Checking with delegate", getIdentifier());
                return getDelegateRecord().getLength();
            }
            return size;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            return getDelegateRecord().getStream();
        }

        @Override
        public long getLastModified() {
            try {
                return getDelegateRecord().getLastModified();
            } catch (DataStoreException e) {
                throw new RuntimeException(e);
            }
        }

        private DataRecord getDelegateRecord() throws DataStoreException {
            //Lazily load the delegateRecord to avoid FS access
            if (delegateRecord == null) {
                delegateRecord = getDelegate().getRecord(getIdentifier());
            }
            return delegateRecord;
        }
    }

    //~---------------------------------< internal >

    private void checkIfReadOnly() {
        checkState(!readOnly, "Read only DataStore in use");
    }

    private DataStore getDelegate() {
        return checkNotNull(delegate, "Delegate DataStore not configured");
    }

    private void addNewMapping(DataRecord dr) throws DataStoreException {
        if (dr != null) {
            newMappings.put(dr.getIdentifier().toString(), dr.getLength());
        }
    }

    private void initializeMappingData(String homeDir) {
        mappingFile = new File(FilenameUtils.concat(homeDir, mappingFilePath));
        if (mappingFile.exists()) {
            try {
                existingMappings = loadMappingData(mappingFile);
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Failed to read mapping data from " + mappingFile, e);
            }
        } else {
            log.info("Mapping file {} not found. Would create a new one.", mappingFile);
        }
    }

    private void initializeDelegate(String homeDir) throws RepositoryException {
        checkNotNull(delegateClass, "No delegate DataStore class defined via 'delegateClass' property");
        try {
            delegate = (DataStore) getClass().getClassLoader().loadClass(delegateClass).newInstance();
        } catch (InstantiationException e) {
            throw new RepositoryException("Cannot load delegate class " + delegateClass, e);
        } catch (IllegalAccessException e) {
            throw new RepositoryException("Cannot load delegate class " + delegateClass, e);
        } catch (ClassNotFoundException e) {
            throw new RepositoryException("Cannot load delegate class " + delegateClass, e);
        }

        log.info("Using {} as the delegating DataStore", delegateClass);
        if (delegateConfigFilePath != null) {
            File configFile = new File(delegateConfigFilePath);
            checkArgument(configFile.exists(), "Delegate DataStore config file %s does not exist", configFile.getAbsolutePath());

            InputStream is = null;
            try {
                Properties props = new Properties();
                is = Files.asByteSource(configFile).openStream();
                props.load(is);
                PropertiesUtil.populate(delegate, propsToMap(props), false);
                log.info("Configured the delegating DataStore via {}", configFile.getAbsolutePath());
            } catch (IOException e) {
                throw new RepositoryException("Error reading from config file " + configFile.getAbsolutePath(), e);
            } finally {
                IOUtils.closeQuietly(is);
            }
        }

        delegate.init(homeDir);
    }

    private void saveNewMappingsToFile() {
        if (!newMappings.isEmpty()) {
            BufferedWriter w = null;
            try {
                w = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(mappingFile, true), Charsets.UTF_8));
                for (Map.Entry<String, Long> e : newMappings.entrySet()) {
                    w.write(String.valueOf(e.getValue()));
                    w.write(SEPARATOR);
                    w.write(e.getKey());
                    w.newLine();
                }
                log.info("Added {} new entries to the mapping file {}", newMappings.size(), mappingFile);
                newMappings.clear();
            } catch (IOException e) {
                log.warn("Error occurred while writing mapping data to {}", mappingFile, e);
            } finally {
                IOUtils.closeQuietly(w);
            }
        }
    }

    private static Map<String, Long> loadMappingData(File mappingFile) throws FileNotFoundException {
        Map<String, Long> mapping = new HashMap<String, Long>();
        log.info("Reading mapping data from {}", mappingFile.getAbsolutePath());
        LineIterator itr = new LineIterator(Files.newReader(mappingFile, Charsets.UTF_8));
        try {
            while (itr.hasNext()) {
                String line = itr.nextLine();
                int indexOfBar = line.indexOf(SEPARATOR);
                checkState(indexOfBar > 0, "Malformed entry found [%s]", line);
                String length = line.substring(0, indexOfBar);
                String id = line.substring(indexOfBar + 1);
                mapping.put(id.trim(), Long.valueOf(length));
            }
            log.info("Total {} mapping entries found", mapping.size());
        } finally {
            itr.close();
        }
        return mapping;
    }

    private static Map<String, Object> propsToMap(Properties p) {
        Map<String, Object> result = Maps.newHashMap();
        for (String keyName : p.stringPropertyNames()) {
            result.put(keyName, p.getProperty(keyName));
        }
        return result;
    }
}
