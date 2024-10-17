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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.io.BaseEncoding;
import org.apache.jackrabbit.guava.common.io.Closeables;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataRecord;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.commons.io.FileTreeTraverser;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.commons.io.FilenameUtils.normalizeNoEndSeparator;

/**
 *  Oak specific extension of JR2 FileDataStore which enables
 *  provisioning the signing key via OSGi config
 */
public class OakFileDataStore extends FileDataStore implements SharedDataStore {
    public static final Logger LOG = LoggerFactory.getLogger(OakFileDataStore.class);
    private static final int DEFAULT_MIN_RECORD_LENGTH = 4096;

    private byte[] referenceKey;

    public OakFileDataStore() {
        //TODO FIXME Temporary workaround for OAK-1666. Override the default
        //synchronized map with a Noop. This should be removed when fix
        //for JCR-3764 is part of release.
        inUse = new NoOpMap<DataIdentifier, WeakReference<DataIdentifier>>();
        // Set default min record length overiding the 100 set for FileDataStore
        setMinRecordLength(DEFAULT_MIN_RECORD_LENGTH);
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() {
        final String path = normalizeNoEndSeparator(new File(getPath()).getAbsolutePath());

        return FileTreeTraverser.depthFirstPostOrder(new File(path))
                .filter(file -> file.isFile() && !file.getParent().equals(path))
                .map(file -> new DataIdentifier(file.getName()))
                .iterator();
    }

    @Override
    protected byte[] getOrCreateReferenceKey() throws DataStoreException {
        if (referenceKey != null) {
            return referenceKey;
        }
        return super.getOrCreateReferenceKey();
    }

    /**
     * Set Base64 encoded signing key
     */
    public void setReferenceKeyEncoded(String encodedKey) {
        this.referenceKey = BaseEncoding.base64().decode(encodedKey);
    }

    /**
     * Set the referenceKey from plain text. Key content would be
     * UTF-8 encoding of the string.
     *
     * <p>This is useful when setting key via generic
     *  bean property manipulation from string properties. User can specify the
     *  key in plain text and that would be passed on this object via
     *  {@link org.apache.jackrabbit.oak.commons.PropertiesUtil#populate(Object, java.util.Map, boolean)}
     *
     * @param textKey base64 encoded key
     * @see org.apache.jackrabbit.oak.commons.PropertiesUtil#populate(Object, java.util.Map, boolean)
     */
    public void setReferenceKeyPlainText(String textKey) {
        this.referenceKey = textKey.getBytes(StandardCharsets.UTF_8);
    }

    public void setReferenceKey(byte[] referenceKey) {
        this.referenceKey = referenceKey;
    }

    /**
     * Noop map which eats up all the put call
     */
    static class NoOpMap<K, V> extends AbstractMap<K, V> {

        @Override
        public V put(K key, V value) {
            //Eat the put call
            return null;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return Collections.emptySet();
        }
    }

    @Override
    public void addMetadataRecord(InputStream input, String name)
            throws DataStoreException {
        checkArgument(input != null, "input should not be null");
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

        try {
            File file = new File(getPath(), name);
            FileOutputStream os = new FileOutputStream(file);
            try {
                IOUtils.copyLarge(input, os);
            } finally {
                Closeables.close(os, true);
                Closeables.close(input, true);
            }
        } catch (IOException e) {
            LOG.error("Exception while adding metadata record with name {}, {}",
                    new Object[] {name, e});
            throw new DataStoreException("Could not add root record", e);
        }
    }

    @Override
    public void addMetadataRecord(File input, String name) throws DataStoreException {
        checkArgument(input != null, "input should not be null");
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

        try {
            File file = new File(getPath(), name);
            FileUtils.copyFile(input, file);
        } catch (IOException e) {
            LOG.error("Exception while adding metadata record file {} with name {}, {}",
                new Object[] {input, name, e});
            throw new DataStoreException("Could not add root record", e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

        File root = new File(getPath());
        for (File file : FileFilterUtils.filter(FileFilterUtils.nameFileFilter(name), root.listFiles())) {
            if (!file.isDirectory()) {
                return new FileDataRecord(this, new DataIdentifier(file.getName()), file);
            }
        }
        return null;
    }

    @Override
    public boolean metadataRecordExists(String name) {
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

        File root = new File(getPath());

        for (File file : FileFilterUtils.filterList(
            FileFilterUtils.nameFileFilter(name),
            root.listFiles())) {
            if (!file.isDirectory()) { // skip directories which are actual data store files
                if (!file.exists()) {
                    LOG.debug("File does not exist {} ",
                        new Object[] {file.getAbsolutePath()});
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        checkArgument(null != prefix, "prefix should not be null");

        File root = new File(getPath());
        List<DataRecord> rootRecords = new ArrayList<DataRecord>();
        for (File file : FileFilterUtils.filterList(
                FileFilterUtils.prefixFileFilter(prefix),
                root.listFiles())) {
            if (!file.isDirectory()) { // skip directories which are actual data store files
                rootRecords.add(
                        new FileDataRecord(this, new DataIdentifier(file.getName()), file));
            }
        }
        return rootRecords;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

        File root = new File(getPath());

        for (File file : FileFilterUtils.filterList(
                FileFilterUtils.nameFileFilter(name),
                root.listFiles())) {
            if (!file.isDirectory()) { // skip directories which are actual data store files
                if (!file.delete()) {
                    LOG.warn("Failed to delete root record {} ", new Object[] {file
                            .getAbsolutePath()});
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        checkArgument(null != prefix, "prefix should not be empty");

        File root = new File(getPath());

        for (File file : FileFilterUtils.filterList(
                FileFilterUtils.prefixFileFilter(prefix),
                root.listFiles())) {
            if (!file.isDirectory()) { // skip directories which are actual data store files
                if (!file.delete()) {
                    LOG.warn("Failed to delete root record {} ", new Object[] {file
                            .getAbsolutePath()});
                }
            }
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        final String path = normalizeNoEndSeparator(new File(getPath()).getAbsolutePath());
        final OakFileDataStore store = this;

        return FileTreeTraverser.depthFirstPostOrder(new File(path))
                .filter(file -> file.isFile() && !file.getParent().equals(path))
                .map(file -> (DataRecord) new FileDataRecord(store, new DataIdentifier(file.getName()), file))
                .iterator();
    }

    @Override
    public DataRecord getRecordForId(DataIdentifier id) throws DataStoreException {
        return getRecord(id);
    }

    @Override
    public Type getType() {
        return Type.SHARED;
    }
}
