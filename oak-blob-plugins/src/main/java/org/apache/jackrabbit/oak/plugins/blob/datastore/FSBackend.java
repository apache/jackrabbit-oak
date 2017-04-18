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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.LazyFileInputStream;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.io.FilenameUtils.normalizeNoEndSeparator;

/**
 */
public class FSBackend extends AbstractSharedBackend {
    private static final Logger LOG = LoggerFactory.getLogger(FSBackend.class);

    public static final String FS_BACKEND_PATH = "fsBackendPath";
    /**
     * The maximum last modified time resolution of the file system.
     */
    private static final int ACCESS_TIME_RESOLUTION = 2000;

    private Properties properties;

    private String fsPath;

    private File fsPathDir;

    @Override
    public void init() throws DataStoreException {
        fsPath = properties.getProperty(FS_BACKEND_PATH);
        if (this.fsPath == null || "".equals(this.fsPath)) {
            throw new DataStoreException(
                "Could not initialize FSBackend from " + properties + ". [" + FS_BACKEND_PATH
                    + "] property not found.");
        }
        this.fsPath = normalizeNoEndSeparator(fsPath);
        fsPathDir = new File(this.fsPath);
        if (fsPathDir.exists() && fsPathDir.isFile()) {
            throw new DataStoreException(
                "Can not create a directory " + "because a file exists with the same name: "
                    + this.fsPath);
        }
        if (!fsPathDir.exists()) {
            boolean created = fsPathDir.mkdirs();
            if (!created) {
                throw new DataStoreException(
                    "Could not create directory: " + fsPathDir.getAbsolutePath());
            }
        }
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        File file = getFile(identifier, fsPathDir);
        try {
            return new LazyFileInputStream(file);
        } catch (IOException e) {
            throw new DataStoreException("Error opening input stream of " + file.getAbsolutePath(),
                e);
        }
    }

    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        File dest = getFile(identifier, fsPathDir);
        synchronized (this) {
            if (dest.exists()) {
                long now = System.currentTimeMillis();
                if (getLastModified(dest) < now + ACCESS_TIME_RESOLUTION) {
                    setLastModified(dest, now + ACCESS_TIME_RESOLUTION);
                }
            } else {
                try {
                    FileUtils.copyFile(file, dest);
                } catch (IOException ie) {
                    LOG.error("failed to copy [{}] to [{}]", file.getAbsolutePath(),
                        dest.getAbsolutePath());
                    throw new DataStoreException("Not able to write file [" + identifier + "]", ie);
                }
            }
        }
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();

        File file = getFile(identifier, fsPathDir);
        if (!file.exists() || !file.isFile()) {
            LOG.info("getRecord:Identifier [{}] not found. Took [{}] ms.", identifier,
                (System.currentTimeMillis() - start));
            throw new DataStoreException("Identifier [" + identifier + "] not found.");
        }
        return new FSBackendDataRecord(this, identifier, file);
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return Files.fileTreeTraverser().postOrderTraversal(fsPathDir)
            .filter(new Predicate<File>() {
                @Override public boolean apply(File input) {
                    return input.isFile() && !normalizeNoEndSeparator(input.getParent())
                        .equals(fsPath);
                }
            }).transform(new Function<File, DataIdentifier>() {
                @Override public DataIdentifier apply(File input) {
                    return new DataIdentifier(input.getName());
                }
            }).iterator();
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        File file = getFile(identifier, fsPathDir);
        return file.exists() && file.isFile();
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        File file = getFile(identifier, fsPathDir);
        synchronized (this) {
            if (file.exists()) {
                if (file.delete()) {
                    deleteEmptyParentDirs(file);
                } else {
                    LOG.warn("Failed to delete file " + file.getAbsolutePath());
                }
            }
        }
    }

    @Override
    public void addMetadataRecord(InputStream input, String name)
        throws DataStoreException {
        try {
            File file = new File(fsPathDir, name);
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
        try {
            File file = new File(fsPathDir, name);
            FileUtils.copyFile(input, file);
        } catch (IOException e) {
            LOG.error("Exception while adding metadata record file {} with name {}, {}",
                input, name, e);
            throw new DataStoreException("Could not add root record", e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        for (File file : FileFilterUtils
            .filter(FileFilterUtils.nameFileFilter(name), fsPathDir.listFiles())) {
            if (!file.isDirectory()) {
                return new FSBackendDataRecord(this, new DataIdentifier(file.getName()), file);
            }
        }
        return null;
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        List<DataRecord> rootRecords = new ArrayList<DataRecord>();
        for (File file : FileFilterUtils
            .filterList(FileFilterUtils.prefixFileFilter(prefix), fsPathDir.listFiles())) {
            if (!file.isDirectory()) { // skip directories which are actual data store files
                rootRecords
                    .add(new FSBackendDataRecord(this, new DataIdentifier(file.getName()), file));
            }
        }
        return rootRecords;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        for (File file : FileFilterUtils
            .filterList(FileFilterUtils.nameFileFilter(name), fsPathDir.listFiles())) {
            if (!file.isDirectory()) { // skip directories which are actual data store files
                if (!file.delete()) {
                    LOG.warn("Failed to delete root record {} ",
                        new Object[] {file.getAbsolutePath()});
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        for (File file : FileFilterUtils
            .filterList(FileFilterUtils.prefixFileFilter(prefix), fsPathDir.listFiles())) {
            if (!file.isDirectory()) { // skip directories which are actual data store files
                if (!file.delete()) {
                    LOG.warn("Failed to delete root record {} ",
                        new Object[] {file.getAbsolutePath()});
                }
            }
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        final AbstractSharedBackend backend = this;
        return Files.fileTreeTraverser().postOrderTraversal(fsPathDir)
            .filter(new Predicate<File>() {
                @Override public boolean apply(File input) {
                    return input.isFile() && !normalizeNoEndSeparator(input.getParent())
                        .equals(fsPath);
                }
            }).transform(new Function<File, DataRecord>() {
                @Override public DataRecord apply(File input) {
                    return new FSBackendDataRecord(backend, new DataIdentifier(input.getName()),
                        input);
                }
            }).iterator();
    }


    @Override
    public void close() throws DataStoreException {
    }

    @Override
    public byte[] getOrCreateReferenceKey() throws DataStoreException {
        File file = new File(fsPathDir, "reference.key");
        try {
            if (file.exists()) {
                return FileUtils.readFileToByteArray(file);
            } else {
                byte[] key = super.getOrCreateReferenceKey();
                FileUtils.writeByteArrayToFile(file, key);
                return key;
            }
        } catch (IOException e) {
            throw new DataStoreException("Unable to access reference key file " + file.getPath(),
                e);
        }
    }

    /*----------------------------------- Helper Methods-- -------------------------------------**/

    /**
     * Returns the identified file. This method implements the pattern used to
     * avoid problems with too many files in a single directory.
     * <p>
     * No sanity checks are performed on the given identifier.
     *
     * @param identifier data identifier
     * @return identified file
     */
    private static File getFile(DataIdentifier identifier, File root) {
        String string = identifier.toString();
        File file = root;
        file = new File(file, string.substring(0, 2));
        file = new File(file, string.substring(2, 4));
        file = new File(file, string.substring(4, 6));
        return new File(file, string);
    }

    /**
     * Get the last modified date of a file.
     *
     * @param file the file
     * @return the last modified date
     * @throws DataStoreException if reading fails
     */
    private static long getLastModified(File file) throws DataStoreException {
        long lastModified = file.lastModified();
        if (lastModified == 0) {
            throw new DataStoreException(
                "Failed to read record modified date: " + file.getAbsolutePath());
        }
        return lastModified;
    }

    /**
     * Set the last modified date of a file, if the file is writable.
     *
     * @param file the file
     * @param time the new last modified date
     * @throws DataStoreException if the file is writable but modifying the date
     *                            fails
     */
    private static void setLastModified(File file, long time) throws DataStoreException {
        if (!file.setLastModified(time)) {
            if (!file.canWrite()) {
                // if we can't write to the file, so garbage collection will
                // also not delete it
                // (read only files or file systems)
                return;
            }
            try {
                // workaround for Windows: if the file is already open for
                // reading
                // (in this or another process), then setting the last modified
                // date
                // doesn't work - see also JCR-2872
                RandomAccessFile r = new RandomAccessFile(file, "rw");
                try {
                    r.setLength(r.length());
                } finally {
                    r.close();
                }
            } catch (IOException e) {
                throw new DataStoreException(
                    "An IO Exception occurred while trying to set the last modified date: " + file
                        .getAbsolutePath(), e);
            }
        }
    }

    private void deleteEmptyParentDirs(File file) {
        File parent = file.getParentFile();
        try {
            // Only iterate & delete if parent directory of the blob file is
            // child
            // of the base directory and if it is empty
            while (FileUtils.directoryContains(fsPathDir, parent)) {
                String[] entries = parent.list();
                if (entries == null) {
                    LOG.warn("Failed to list directory {}", parent.getAbsolutePath());
                    break;
                }
                if (entries.length > 0) {
                    break;
                }
                boolean deleted = parent.delete();
                LOG.debug("Deleted parent [{}] of file [{}]: {}",
                    parent, file.getAbsolutePath(), deleted);
                parent = parent.getParentFile();
            }
        } catch (IOException e) {
            LOG.warn("Error in parents deletion for " + file.getAbsoluteFile(), e);
        }
    }

    /*--------------------------------- Gettters & Setters -------------------------------------**/

    /**
     * Properties used to configure the backend. These are mandatorily to be provided explicitly
     * before calling {{@link #init()} is invoked.
     *
     * @param properties to configure Backend
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /*-------------------------------- Inner classes -------------------------------------------**/

    /**
     * FSBackendDataRecord which lazily retrieves the input stream of the record.
     */
    class FSBackendDataRecord extends AbstractDataRecord {
        private long length;
        private long lastModified;
        private File file;

        public FSBackendDataRecord(AbstractSharedBackend backend,
            @Nonnull DataIdentifier identifier, @Nonnull File file) {
            super(backend, identifier);
            this.file = file;
            this.length = file.length();
            this.lastModified = file.lastModified();
        }

        @Override public long getLength() throws DataStoreException {
            return length;
        }

        @Override public InputStream getStream() throws DataStoreException {
            try {
                return new LazyFileInputStream(file);
            } catch (FileNotFoundException e) {
                LOG.error("Error in returning stream", e);
                throw new DataStoreException(e);
            }
        }

        @Override public long getLastModified() {
            return lastModified;
        }

        @Override public String toString() {
            return "S3DataRecord{" + "identifier=" + getIdentifier() + ", length=" + length
                + ", lastModified=" + lastModified + '}';
        }
    }
}
