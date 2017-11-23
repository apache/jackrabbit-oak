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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.CheckForNull;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents the index metadata file content as present in index-details.txt
 */
final class IndexMeta implements Comparable<IndexMeta> {
    public static final String DIR_PREFIX = "dir.";
    final String indexPath;
    final long creationTime;
    final int metaFormatVersion = 1;
    private final Map<String, String> properties;

    public IndexMeta(String indexPath) {
        this(indexPath, System.currentTimeMillis(), Collections.emptyMap());
    }

    public IndexMeta(String indexPath, long creationTime) {
        this(indexPath, creationTime, Collections.emptyMap());
    }

    public IndexMeta(String indexPath, long creationTime, Map<String, String> properties) {
        this.indexPath = indexPath;
        this.creationTime = creationTime;
        this.properties = new HashMap<>(properties);
    }

    public IndexMeta(File file) throws IOException {
        Properties p = loadFromFile(file);
        this.indexPath = checkNotNull(p.getProperty("indexPath"));
        this.creationTime = Long.valueOf(checkNotNull(p.getProperty("creationTime")));
        this.properties = new HashMap<>(Maps.fromProperties(p));
    }

    public void addDirectoryMapping(String jcrDirName, String fsDirName){
        properties.put(keyFromFSDirName(fsDirName), jcrDirName);
    }

    @CheckForNull
    public String getJcrNameFromFSName(String fsDirName) {
        String nameKey = keyFromFSDirName(fsDirName);
        for (Map.Entry<String, String> e : properties.entrySet()) {
            if (nameKey.equals(e.getKey())){
                return e.getValue();
            }
        }
        return null;
    }

    /**
     * Returns the file system name for the given JCR name which
     * represents a directory on the file system
     */
    @CheckForNull
    public String getFSNameFromJCRName(String jcrDirName) {
        for (Map.Entry<String, String> e : properties.entrySet()) {
            if (e.getKey().startsWith(DIR_PREFIX) && jcrDirName.equals(e.getValue())){
                return e.getKey().substring(DIR_PREFIX.length());
            }
        }
        return null;
    }

    public void writeTo(File file) throws IOException {
        Properties p = new Properties();
        p.putAll(properties);
        p.setProperty("metaFormatVersion", String.valueOf(metaFormatVersion));
        p.setProperty("indexPath", indexPath);
        p.setProperty("creationTime", String.valueOf(creationTime));
        OutputStream os = null;
        try {
            os = new BufferedOutputStream(new FileOutputStream(file));
            p.store(os, "Index metadata");
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(os);
        }
    }

    @Override
    public int compareTo(IndexMeta o) {
        return Long.compare(creationTime, o.creationTime);
    }

    @Override
    public String toString() {
        return String.format("%s, %tc", indexPath, creationTime);
    }

    private static String keyFromFSDirName(String fsDirName) {
        return DIR_PREFIX + fsDirName;
    }

    private static Properties loadFromFile(File file) throws IOException {
        InputStream is = null;
        try {
            is = FileUtils.openInputStream(file);
            Properties p = new Properties();
            p.load(is);
            return p;
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(is);
        }
    }
}
    