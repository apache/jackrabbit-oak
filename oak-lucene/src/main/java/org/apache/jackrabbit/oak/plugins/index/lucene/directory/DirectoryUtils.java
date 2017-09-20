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
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory.INDEX_METADATA_FILE_NAME;

public class DirectoryUtils {
    /**
     * Get the file length in best effort basis.
     * @return actual fileLength. -1 if cannot determine
     */
    public static long getFileLength(Directory dir, String fileName){
        try{
            //Check for file presence otherwise internally it results in
            //an exception to be created
            if (dir.fileExists(fileName)) {
                return dir.fileLength(fileName);
            }
        } catch (Exception ignore){

        }
        return -1;
    }

    public static long dirSize(Directory directory) throws IOException {
        long totalFileSize = 0L;
        if (directory == null) {
            return -1;
        }
        String[] files = directory.listAll();
        if (files == null) {
            return totalFileSize;
        }
        for (String file : files) {
            totalFileSize += directory.fileLength(file);
        }
        return totalFileSize;
    }

    public static File createIndexDir(File baseDir, String indexPath) throws IOException {
        String subDirPath = IndexRootDirectory.getIndexFolderBaseName(indexPath);
        IndexRootDirectory rootDir = new IndexRootDirectory(baseDir, false);
        List<LocalIndexDir> existingDirs = rootDir.getLocalIndexes(indexPath);
        File indexDir;
        if (existingDirs.isEmpty()) {
            indexDir = new File(baseDir, subDirPath);
            int count = 0;
            while (true) {
                if (indexDir.exists()) {
                    indexDir = new File(baseDir, subDirPath + "_" + count++);
                } else {
                    break;
                }
            }
            FileUtils.forceMkdir(indexDir);
        } else {
            indexDir = existingDirs.get(0).dir;
        }
        return indexDir;
    }

    public static int getNumDocs(Directory dir) throws IOException {
        int count = 0;
        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);

        for (SegmentCommitInfo sci : sis) {
            count += sci.info.getDocCount() - sci.getDelCount();
        }

        return count;
    }

    static File createSubDir(File indexDir, String name) throws IOException {
        String fsSafeName = name.replace(":", "");
        File dir = new File(indexDir, fsSafeName);
        FileUtils.forceMkdir(dir);
        return dir;
    }

    static void writeMeta(File indexDir, IndexMeta meta) throws IOException {
        File readMe = new File(indexDir, INDEX_METADATA_FILE_NAME);
        meta.writeTo(readMe);
    }
}
