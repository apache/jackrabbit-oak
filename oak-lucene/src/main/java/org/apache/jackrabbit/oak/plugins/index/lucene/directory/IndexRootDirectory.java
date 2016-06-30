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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.stats.Clock;

import static com.google.common.base.Preconditions.checkState;

public class IndexRootDirectory {
    public static final String INDEX_METADATA_FILE_NAME = "index-details.txt";

    private final File indexRootDir;

    public IndexRootDirectory(File indexRootDir) {
        this.indexRootDir = indexRootDir;
    }

    public long getSize(){
        return FileUtils.sizeOfDirectory(indexRootDir);
    }

    public File getIndexDir(IndexDefinition definition, String indexPath) throws IOException {
        String uid = definition.getUniqueId();

        if (uid == null) {
            //Old format
            String subDir = getPathHash(indexPath);
            File baseFolder = new File(indexRootDir, subDir);
            String version = String.valueOf(definition.getReindexCount());
            File indexDir = new File(baseFolder, version);
            if (!indexDir.exists()){
                checkState(indexDir.mkdirs(), "Not able to create folder [%s]", indexDir);
            }
            return indexDir;
        } else {
            String fileSystemSafeName = getIndexFolderBaseName(indexPath);
            String folderName = fileSystemSafeName + "-" + uid;
            File baseFolder = new File(indexRootDir, folderName);

            //Create a base folder <index node name>-<uid>
            //and add a readme file having index info
            if (!baseFolder.exists()){
                checkState(baseFolder.mkdir(), "Not able to create folder [%s]", baseFolder);
                File readMe = new File(baseFolder, INDEX_METADATA_FILE_NAME);
                IndexMeta meta = new IndexMeta(indexPath, getTime());
                meta.writeTo(readMe);
            }

            //Create index folder under that
            //TODO Add support for multiple folders depending on type of content
            File indexFolder = new File(baseFolder, "default");
            if (!indexFolder.exists()) {
                checkState(indexFolder.mkdir(), "Not able to create folder [%s]", indexFolder);
            }

            return indexFolder;
        }
    }

    /**
     * Returns the most recent directory for each index. If for an index 2 versions are present
     * then it would return the most recent version
     */
    public List<LocalIndexDir> getAllLocalIndexes() throws IOException {
        Map<String, List<LocalIndexDir>> mapping = getIndexesPerPath();
        List<LocalIndexDir> result = Lists.newArrayListWithCapacity(mapping.size());
        for (Map.Entry<String, List<LocalIndexDir>> e : mapping.entrySet()){
            result.add(e.getValue().get(0));
        }
        return result;
    }

    public List<LocalIndexDir> getLocalIndexes(String jcrPath) throws IOException {
        List<LocalIndexDir> result = getIndexesPerPath().get(jcrPath);
        return result == null ? Collections.<LocalIndexDir>emptyList() : result;
    }

    static String getIndexFolderBaseName(String indexPath) {
        String nodeName = PathUtils.getName(indexPath);

        //Strip of any char outside of a-zA-Z0-9-
        return nodeName.replaceAll("\\W+", "");
    }

    static String getPathHash(String indexPath) {
        return Hashing.sha256().hashString(indexPath, Charsets.UTF_8).toString();
    }

    /**
     * The value is a sorted list with most recent version of index at the start
     */
    private Map<String, List<LocalIndexDir>> getIndexesPerPath() throws IOException {
        File[] dirs = indexRootDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (!file.isDirectory()){
                    return false;
                }
                File metaFile = new File(file, INDEX_METADATA_FILE_NAME);
                return metaFile.exists();
            }
        });

        ListMultimap<String, LocalIndexDir> pathToDirMap = ArrayListMultimap.create();
        for (File indexDir : dirs){
            LocalIndexDir localIndexDir = new LocalIndexDir(indexDir);
            pathToDirMap.get(localIndexDir.getJcrPath()).add(localIndexDir);
        }

        Map<String, List<LocalIndexDir>> result = Maps.newHashMap();
        for (Map.Entry<String, Collection<LocalIndexDir>> e : pathToDirMap.asMap().entrySet()){
            List<LocalIndexDir> sortedDirs = new ArrayList<>(e.getValue());
            Collections.sort(sortedDirs, Collections.<LocalIndexDir>reverseOrder());
            result.put(e.getKey(), sortedDirs);
        }
        return result;
    }

    private static long getTime() {
        try {
            return Clock.SIMPLE.getTimeIncreasing();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Clock.SIMPLE.getTimeMonotonic();
        }
    }
}
