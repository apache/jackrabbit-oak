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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.FilePacker;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.FilePacker.FileEntry;

/**
 * A store where all the entries are stored in a "Pack File" (see @FilePacker).
 * The store is read-only.
 */
public class PackStore implements Store {

    private final Properties config;
    private final String fileName;
    private final File file;
    private final RandomAccessFile f;
    private final HashMap<String, FileEntry> directory = new HashMap<>();
    private final long maxFileSizeBytes;
    private final long start;
    private long readCount;

    public PackStore(Properties config) {
        this.config = config;
        this.fileName = config.getProperty("file");
        this.maxFileSizeBytes = Long.parseLong(config.getProperty(
                Store.MAX_FILE_SIZE_BYTES, "" + Store.DEFAULT_MAX_FILE_SIZE_BYTES));
        try {
            file = new File(fileName);
            f = new RandomAccessFile(fileName, "r");
            ArrayList<FileEntry> list = FilePacker.readDirectoryListing(file, f);
            start = f.getFilePointer();
            for(FileEntry e : list) {
                directory.put(e.fileName, e);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Error reading file " + fileName, e);
        }
    }

    public String toString() {
        return "pack(" + fileName + ")";
    }

    @Override
    public PageFile getIfExists(String key) {
        readCount++;
        FileEntry fe = directory.get(key);
        if (fe == null) {
            return null;
        }
        synchronized (this) {
            try {
                f.seek(start + fe.offset);
                byte[] data = new byte[(int) fe.length];
                f.readFully(data);
                Compression c = Compression.getCompressionFromData(data[0]);
                data = c.expand(data);
                return PageFile.fromBytes(data, maxFileSizeBytes);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    @Override
    public void put(String key, PageFile value) {
        throw new IllegalStateException("Store is read-only");
    }

    @Override
    public String newFileName() {
        throw new IllegalStateException("Store is read-only");
    }

    @Override
    public Set<String> keySet() {
        return directory.keySet();
    }

    @Override
    public void remove(Set<String> set) {
        throw new IllegalStateException("Store is read-only");
    }

    @Override
    public void removeAll() {
        throw new IllegalStateException("Store is read-only");
    }

    @Override
    public long getWriteCount() {
        return 0;
    }

    @Override
    public long getReadCount() {
        return readCount;
    }

    @Override
    public void setWriteCompression(Compression compression) {
        // ignore
    }

    @Override
    public void close() {
        try {
            f.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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
