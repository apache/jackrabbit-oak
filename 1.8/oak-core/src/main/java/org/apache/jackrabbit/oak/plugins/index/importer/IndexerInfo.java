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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.filefilter.DirectoryFileFilter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents the index data created by oak-run tooling on the file system.
 * It looks for 'indexer-info.properties' file in given directory to read the
 * check point information.
 *
 * Then for each sub directory it looks for 'index-details.txt' file
 * which contains index specific implementation details. It looks for
 * property 'indexPath' which is used to associate the index data to
 * index location in repository
 */
public class IndexerInfo {
    /**
     * File name stored in final index directory which contains meta
     * information like checkpoint details. This can be used by
     * importer while importing the indexes
     */
    public static final String INDEXER_META = "indexer-info.properties";

    /**
     * Name of meta file which stores the index related meta information
     * in properties file format
     */
    public static final String INDEX_METADATA_FILE_NAME = "index-details.txt";

    /**
     * Property name in index-details.txt which refers to the
     * index path in repository
     */
    public static final String PROP_INDEX_PATH = "indexPath";

    public final String checkpoint;
    private final File rootDir;

    public IndexerInfo(File rootDir, String checkpoint) {
        this.rootDir = rootDir;
        this.checkpoint = checkNotNull(checkpoint);
    }

    public void save() throws IOException {
        File infoFile = new File(rootDir, INDEXER_META);
        Properties p = new Properties();
        p.setProperty("checkpoint", checkpoint);
        PropUtils.writeTo(p, infoFile, "Indexer info");
    }

    public Map<String, File> getIndexes() throws IOException {
        ImmutableMap.Builder<String, File> indexes = ImmutableMap.builder();
        for (File dir : rootDir.listFiles(((FileFilter) DirectoryFileFilter.DIRECTORY))) {
            File metaFile = new File(dir, INDEX_METADATA_FILE_NAME);
            if (metaFile.exists()) {
                Properties p = PropUtils.loadFromFile(metaFile);
                String indexPath = p.getProperty(PROP_INDEX_PATH);
                if (indexPath != null) {
                    indexes.put(indexPath, dir);
                }
            }
        }
        return indexes.build();
    }

    public static IndexerInfo fromDirectory(File rootDir) throws IOException {
        File infoFile = new File(rootDir, INDEXER_META);
        checkArgument(infoFile.exists(), "No [%s] file found in [%s]. Not a valid exported index " +
                "directory", INDEXER_META, rootDir.getAbsolutePath());
        Properties p = PropUtils.loadFromFile(infoFile);
        return new IndexerInfo(
                rootDir,
                PropUtils.getProp(p, "checkpoint")
        );
    }
}
