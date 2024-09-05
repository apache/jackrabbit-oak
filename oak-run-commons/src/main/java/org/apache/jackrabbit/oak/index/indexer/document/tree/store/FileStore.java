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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.TimeUuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage backend for the tree store that stores files on the local file
 * system.
 *
 * This is the main (read-write) storage backend.
 */
public class FileStore implements Store {

    private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

    private final Properties config;
    private final String directory;
    private Compression compression = Compression.NO;
    private long writeCount, readCount;
    private final long maxFileSizeBytes;

    public String toString() {
        return "file(" + directory + ")";
    }

    public FileStore(Properties config) {
        this.config = config;
        this.directory = config.getProperty("dir");
        this.maxFileSizeBytes = Long.parseLong(config.getProperty(
                Store.MAX_FILE_SIZE_BYTES, "" + Store.DEFAULT_MAX_FILE_SIZE_BYTES));
        LOG.info("Max file size {} bytes", maxFileSizeBytes);
        new File(directory).mkdirs();
    }

    @Override
    public void close() {
    }

    @Override
    public void setWriteCompression(Compression compression) {
        this.compression = compression;
    }

    @Override
    public PageFile getIfExists(String key) {
        readCount++;
        File f = getFile(key);
        if (!f.exists()) {
            return null;
        }
        try (RandomAccessFile file = new RandomAccessFile(f, "r")) {
            long length = file.length();
            if (length == 0) {
                // deleted in the meantime
                return null;
            }
            byte[] data = new byte[(int) length];
            file.readFully(data);
            Compression c = Compression.getCompressionFromData(data[0]);
            data = c.expand(data);
            return PageFile.fromBytes(data, maxFileSizeBytes);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void put(String key, PageFile value) {
        writeCount++;
        writeFile(key, value.toBytes());
    }

    @Override
    public boolean supportsByteOperations() {
        return true;
    }

    @Override
    public byte[] getBytes(String key) {
        File f = getFile(key);
        if (!f.exists()) {
            return null;
        }
        try {
            readCount++;
            try (RandomAccessFile file = new RandomAccessFile(f, "r")) {
                long length = file.length();
                if (length == 0) {
                    // deleted in the meantime
                    return null;
                }
                byte[] data = new byte[(int) length];
                file.readFully(data);
                return data;
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void putBytes(String key, byte[] data) {
        try (FileOutputStream out = new FileOutputStream(getFile(key))) {
            out.write(data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void writeFile(String key, byte[] data) {
        data = compression.compress(data);
        putBytes(key, data);
    }

    private File getFile(String key) {
        return new File(directory, key);
    }

    @Override
    public String newFileName() {
        return TimeUuid.newUuid().toShortString();
    }

    @Override
    public Set<String> keySet() {
        File dir = new File(directory);
        if (!dir.exists()) {
            return Collections.emptySet();
        }
        String[] list = dir.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isFile();
            }

        });
        return new HashSet<>(Arrays.asList(list));
    }

    @Override
    public void remove(Set<String> set) {
        for (String key : set) {
            writeCount++;
            getFile(key).delete();
        }
    }

    @Override
    public void removeAll() {
        File dir = new File(directory);
        for(File f: dir.listFiles()) {
            f.delete();
        }
    }

    @Override
    public long getWriteCount() {
        return writeCount;
    }

    @Override
    public long getReadCount() {
        return readCount;
    }

    @Override
    public Properties getConfig() {
        return config;
    }

    @Override
    public long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }

}
