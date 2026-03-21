/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * File system {@link Backend} used with {@link CachingDataStore}. 
 * The file system can be network storage.
 */
package org.apache.jackrabbit.core.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSBackend extends AbstractBackend {

    private Properties properties;

    private String fsPath;

    File fsPathDir;

    public static final String FS_BACKEND_PATH = "fsBackendPath";

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(FSBackend.class);

    /**
     * The maximum last modified time resolution of the file system.
     */
    private static final int ACCESS_TIME_RESOLUTION = 2000;

    @Override
    public void init(CachingDataStore store, String homeDir, String config)
                    throws DataStoreException {
        super.init(store, homeDir, config);
        Properties initProps = null;
        // Check is configuration is already provided. That takes precedence
        // over config provided via file based config
        if (this.properties != null) {
            initProps = this.properties;
        } else {
            initProps = new Properties();
            InputStream in = null;
            try {
                in = new FileInputStream(config);
                initProps.load(in);
            } catch (IOException e) {
                throw new DataStoreException(
                    "Could not initialize FSBackend from " + config, e);
            } finally {
                IOUtils.closeQuietly(in);
            }
            this.properties = initProps;
        }
        init(store, homeDir, initProps);

    }

    public void init(CachingDataStore store, String homeDir, Properties prop)
                    throws DataStoreException {
        setDataStore(store);
        setHomeDir(homeDir);
        this.fsPath = prop.getProperty(FS_BACKEND_PATH);
        if (this.fsPath == null || "".equals(this.fsPath)) {
            throw new DataStoreException("Could not initialize FSBackend from "
                + getConfig() + ". [" + FS_BACKEND_PATH + "] property not found.");
        }
        fsPathDir = new File(this.fsPath);
        if (fsPathDir.exists() && fsPathDir.isFile()) {
            throw new DataStoreException("Can not create a directory "
                + "because a file exists with the same name: " + this.fsPath);
        }
        if (!fsPathDir.exists()) {
            boolean created = fsPathDir.mkdirs();
            if (!created) {
                throw new DataStoreException("Could not create directory: "
                    + fsPathDir.getAbsolutePath());
            }
        }
    }

    @Override
    public InputStream read(DataIdentifier identifier)
                    throws DataStoreException {
        File file = getFile(identifier);
        try {
            return new LazyFileInputStream(file);
        } catch (IOException e) {
            throw new DataStoreException("Error opening input stream of "
                + file.getAbsolutePath(), e);
        }
    }

    @Override
    public long getLength(DataIdentifier identifier) throws DataStoreException {
        File file = getFile(identifier);
        if (file.isFile()) {
            return file.length();
        }
        throw new DataStoreException("Could not length of dataIdentifier ["
            + identifier + "]");
    }

    @Override
    public long getLastModified(DataIdentifier identifier)
                    throws DataStoreException {
        long start = System.currentTimeMillis();
        File f = getFile(identifier);
        if (f.isFile()) {
            return getLastModified(f);
        }
        LOG.info("getLastModified:Identifier [{}] not found. Took [{}] ms.",
            identifier, (System.currentTimeMillis() - start));
        throw new DataStoreException("Identifier [" + identifier
            + "] not found.");
    }

    @Override
    public void write(DataIdentifier identifier, File src)
                    throws DataStoreException {
        File dest = getFile(identifier);
        synchronized (this) {
            if (dest.exists()) {
                long now = System.currentTimeMillis();
                if (getLastModified(dest) < now + ACCESS_TIME_RESOLUTION) {
                    setLastModified(dest, now + ACCESS_TIME_RESOLUTION);
                }
            } else {
                try {
                    FileUtils.copyFile(src, dest);
                } catch (IOException ioe) {
                    LOG.error("failed to copy [{}] to [{}]",
                        src.getAbsolutePath(), dest.getAbsolutePath());
                    throw new DataStoreException("Not able to write file ["
                        + identifier + "]", ioe);
                }
            }
        }

    }

    @Override
    public void writeAsync(final DataIdentifier identifier, final File src,
                    final AsyncUploadCallback callback)
                    throws DataStoreException {
        if (callback == null) {
            throw new IllegalArgumentException(
                "callback parameter cannot be null in asyncUpload");
        }
        getAsyncWriteExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    write(identifier, src);
                    callback.onSuccess(new AsyncUploadResult(identifier, src));
                } catch (DataStoreException dse) {
                    AsyncUploadResult res = new AsyncUploadResult(identifier,
                        src);
                    res.setException(dse);
                    callback.onFailure(res);
                }

            }
        });
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers()
                    throws DataStoreException {
        ArrayList<File> files = new ArrayList<File>();
        for (File file : fsPathDir.listFiles()) {
            if (file.isDirectory()) { // skip top-level files
                listRecursive(files, file);
            }
        }

        ArrayList<DataIdentifier> identifiers = new ArrayList<DataIdentifier>();
        for (File f : files) {
            String name = f.getName();
            identifiers.add(new DataIdentifier(name));
        }
        LOG.debug("Found " + identifiers.size() + " identifiers.");
        return identifiers.iterator();
    }

    @Override
    public boolean exists(DataIdentifier identifier, boolean touch)
                    throws DataStoreException {
        File file = getFile(identifier);
        if (file.isFile()) {
            if (touch) {
                long now = System.currentTimeMillis();
                setLastModified(file, now + ACCESS_TIME_RESOLUTION);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        return exists(identifier, false);
    }

    @Override
    public void touch(DataIdentifier identifier, long minModifiedDate)
                    throws DataStoreException {
        File file = getFile(identifier);
        long now = System.currentTimeMillis();
        if (minModifiedDate > 0 && minModifiedDate > getLastModified(file)) {
            setLastModified(file, now + ACCESS_TIME_RESOLUTION);
        }
    }

    @Override
    public void touchAsync(final DataIdentifier identifier,
                    final long minModifiedDate,
                    final AsyncTouchCallback callback)
                    throws DataStoreException {
        try {
            if (callback == null) {
                throw new IllegalArgumentException(
                    "callback parameter cannot be null in touchAsync");
            }
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());

            getAsyncWriteExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        touch(identifier, minModifiedDate);
                        callback.onSuccess(new AsyncTouchResult(identifier));
                    } catch (DataStoreException e) {
                        AsyncTouchResult result = new AsyncTouchResult(
                            identifier);
                        result.setException(e);
                        callback.onFailure(result);
                    }
                }
            });
        } catch (Exception e) {
            if (callback != null) {
                callback.onAbort(new AsyncTouchResult(identifier));
            }
            throw new DataStoreException("Cannot touch the record "
                + identifier.toString(), e);
        }

    }

    @Override
    public Set<DataIdentifier> deleteAllOlderThan(long min)
                    throws DataStoreException {
        Set<DataIdentifier> deleteIdSet = new HashSet<DataIdentifier>(30);
        for (File file : fsPathDir.listFiles()) {
            if (file.isDirectory()) { // skip top-level files
                deleteOlderRecursive(file, min, deleteIdSet);
            }
        }
        return deleteIdSet;
    }

    @Override
    public void deleteRecord(DataIdentifier identifier)
                    throws DataStoreException {
        File file = getFile(identifier);
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

    /**
     * Properties used to configure the backend. If provided explicitly before
     * init is invoked then these take precedence
     * @param properties to configure S3Backend
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * Returns the identified file. This method implements the pattern used to
     * avoid problems with too many files in a single directory.
     * <p>
     * No sanity checks are performed on the given identifier.
     * @param identifier data identifier
     * @return identified file
     */
    private File getFile(DataIdentifier identifier) {
        String string = identifier.toString();
        File file = this.fsPathDir;
        file = new File(file, string.substring(0, 2));
        file = new File(file, string.substring(2, 4));
        file = new File(file, string.substring(4, 6));
        return new File(file, string);
    }

    /**
     * Set the last modified date of a file, if the file is writable.
     * @param file the file
     * @param time the new last modified date
     * @throws DataStoreException if the file is writable but modifying the date
     *             fails
     */
    private static void setLastModified(File file, long time)
                    throws DataStoreException {
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
                    "An IO Exception occurred while trying to set the last modified date: "
                        + file.getAbsolutePath(), e);
            }
        }
    }

    /**
     * Get the last modified date of a file.
     * @param file the file
     * @return the last modified date
     * @throws DataStoreException if reading fails
     */
    private static long getLastModified(File file) throws DataStoreException {
        long lastModified = file.lastModified();
        if (lastModified == 0) {
            throw new DataStoreException(
                "Failed to read record modified date: "
                    + file.getAbsolutePath());
        }
        return lastModified;
    }

    private void listRecursive(List<File> list, File file) {
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    listRecursive(list, f);
                } else {
                    list.add(f);
                }
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
                    LOG.warn("Failed to list directory {}",
                        parent.getAbsolutePath());
                    break;
                }
                if (entries.length > 0) {
                    break;
                }
                boolean deleted = parent.delete();
                LOG.debug("Deleted parent [{}] of file [{}]: {}", new Object[] {
                    parent, file.getAbsolutePath(), deleted });
                parent = parent.getParentFile();
            }
        } catch (IOException e) {
            LOG.warn("Error in parents deletion for " + file.getAbsoluteFile(),
                e);
        }
    }

    private void deleteOlderRecursive(File file, long min,
                    Set<DataIdentifier> deleteIdSet) throws DataStoreException {
        if (file.isFile() && file.exists() && file.canWrite()) {
            synchronized (this) {
                long lastModified;
                try {
                    lastModified = getLastModified(file);
                } catch (DataStoreException e) {
                    LOG.warn(
                        "Failed to read modification date; file not deleted", e);
                    // don't delete the file, since the lastModified date is
                    // uncertain
                    lastModified = min;
                }
                if (lastModified < min) {
                    DataIdentifier id = new DataIdentifier(file.getName());
                    if (getDataStore().confirmDelete(id)) {
                        getDataStore().deleteFromCache(id);
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Deleting old file "
                                + file.getAbsolutePath() + " modified: "
                                + new Timestamp(lastModified).toString()
                                + " length: " + file.length());
                        }
                        if (file.delete()) {
                            deleteIdSet.add(id);
                        } else {
                            LOG.warn("Failed to delete old file "
                                + file.getAbsolutePath());
                        }
                    }
                }
            }
        } else if (file.isDirectory()) {
            File[] list = file.listFiles();
            if (list != null) {
                for (File f : list) {
                    deleteOlderRecursive(f, min, deleteIdSet);
                }
            }

            // JCR-1396: FileDataStore Garbage Collector and empty directories
            // Automatic removal of empty directories (but not the root!)
            synchronized (this) {
                list = file.listFiles();
                if (list != null && list.length == 0) {
                    file.delete();
                }
            }
        }
    }
}
