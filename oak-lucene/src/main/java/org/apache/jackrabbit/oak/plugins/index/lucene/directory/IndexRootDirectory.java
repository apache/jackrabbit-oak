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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndex;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Represents the root directory on file system used for storing index copy locally.
 * For each Oak index in repository it creates a container directory which is a function of
 * index path and a unique id which stored in index node in Oak. Under that container
 * directory various sub directories can be created for storing different types of indexes
 */
public class IndexRootDirectory {
    static final int MAX_NAME_LENGTH = 127;
    public static final String INDEX_METADATA_FILE_NAME = "index-details.txt";

    private static final FileFilter LOCAL_DIR_FILTER = new FileFilter() {
        @Override
        public boolean accept(File file) {
            if (!file.isDirectory()) {
                return false;
            }
            File metaFile = new File(file, INDEX_METADATA_FILE_NAME);
            return metaFile.exists();
        }
    };

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final File indexRootDir;

    public IndexRootDirectory(File indexRootDir) throws IOException {
        this(indexRootDir, true);
    }

    public IndexRootDirectory(File indexRootDir, boolean gcOnStart) throws IOException {
        this.indexRootDir = indexRootDir;
        if (gcOnStart) {
            gcIndexDirs();
        }
    }

    public long getSize(){
        return FileUtils.sizeOfDirectory(indexRootDir);
    }

    public File getIndexDir(IndexDefinition definition, String indexPath, String dirName) throws IOException {
        String uid = definition.getUniqueId();

        if (uid == null) {
            //Old format
            File baseFolder = getOldFormatDir(indexPath);
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
            File indexFolder = new File(baseFolder, getFSSafeName(dirName));
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

    /**
     * Performs garbage collection of older version of index directories based on
     * index directory derived from the passed sub directory.
     *
     * @param subDir one of the sub directories like 'default' etc. Such that
     *               correct local index directory (container dir) can be checked for deletion
     */
    public long gcEmptyDirs(File subDir) throws IOException {
        File parent = checkNotNull(subDir).getParentFile().getCanonicalFile();
        LocalIndexDir indexDir = findMatchingIndexDir(parent);
        long totalDeletedSize = 0;
        if (indexDir != null) {
            List<LocalIndexDir> idxDirs = getLocalIndexes(indexDir.getJcrPath());
            //Flag to determine in given ordered list of LocalIndexDir
            //we found the dir which matched the parent of passed dir. So its safe
            //to delete those dirs and its successors in the list (as they are older)
            boolean matchingDirFound = false;
            for (LocalIndexDir d : idxDirs){
                if (d.dir.equals(parent)){
                    matchingDirFound = true;
                }
                if (matchingDirFound && d.isEmpty()){
                    long dirSize = FileUtils.sizeOf(d.dir);
                    if (FileUtils.deleteQuietly(d.dir)){
                        totalDeletedSize += dirSize;
                    } else {
                        log.warn("Not able to deleted unused local index directory [{}]. " +
                                "Deletion would be retried later again.",  d);
                    }
                    totalDeletedSize += deleteOldFormatDir(d.getJcrPath());
                }
            }
        }
        return totalDeletedSize;
    }


    /**
     * <ul>
     *     <li>abc -> abc</li>
     *     <li>xy:abc -> xyabc</li>
     *     <li>/oak:index/abc -> abc</li>
     * </ul>
     *
     * The resulting file name would be truncated to MAX_NAME_LENGTH
     */
    static String getIndexFolderBaseName(String indexPath) {
        List<String> elements = Lists.newArrayList(PathUtils.elements(indexPath));
        Collections.reverse(elements);
        List<String> result = Lists.newArrayListWithCapacity(2);

        //Max 3 nodeNames including oak:index which is the immediate parent for any indexPath
        for (String e : Iterables.limit(elements, 3)) {
            if ("oak:index".equals(e)) {
                continue;
            }
            //Strip of any char outside of a-zA-Z0-9-
            result.add(getFSSafeName(e));
        }

        Collections.reverse(result);
        String name = Joiner.on('_').join(result);
        if (name.length() > MAX_NAME_LENGTH){
            name = name.substring(0, MAX_NAME_LENGTH);
        }
        return name;
    }

    static String getPathHash(String indexPath) {
        return Hashing.sha256().hashString(indexPath, Charsets.UTF_8).toString();
    }

    /**
     * The value is a sorted list with most recent version of index at the start
     */
    private Map<String, List<LocalIndexDir>> getIndexesPerPath() throws IOException {
        File[] dirs = indexRootDir.listFiles(LOCAL_DIR_FILTER);

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

    /**
     * Garbage collect old index directories. Should only be invoked at startup
     * as it assumes that none of the directories are getting used
     */
    private void gcIndexDirs() throws IOException {
        Map<String, List<LocalIndexDir>> mapping = getIndexesPerPath();
        long totalDeletedSize = 0;
        for (Map.Entry<String, List<LocalIndexDir>> e : mapping.entrySet()) {
            List<LocalIndexDir> dirs = e.getValue();
            //In startup mode we can be sure that no directory is in use
            //so be more aggressive in what we delete i.e. even not empty dir
            for (int i = 1; i < dirs.size(); i++) {
                LocalIndexDir dir = dirs.get(i);
                long dirSize = FileUtils.sizeOf(dir.dir);
                if (FileUtils.deleteQuietly(dir.dir)){
                    totalDeletedSize += dirSize;
                } else {
                    log.warn("Not able to deleted unused local index directory [{}]. " +
                            "Deletion would be retried later again.",  dir);
                }
            }

            if (!dirs.isEmpty()) {
                totalDeletedSize += gcNRTIndexDirs(dirs.get(0));
            }
            totalDeletedSize += deleteOldFormatDir(dirs.get(0).getJcrPath());
        }

        if (totalDeletedSize > 0){
            log.info("Reclaimed [{}] space by removing unused/old index directories",
                    IOUtils.humanReadableByteCount(totalDeletedSize));
        }
    }

    /**
     * Removes all directory created by NRTIndex which have
     * nrt prefix
     */
    private long gcNRTIndexDirs(LocalIndexDir idxDir) {
        final String prefix = getFSSafeName(NRTIndex.NRT_DIR_PREFIX);
        File[] nrtDirs = idxDir.dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(prefix);
            }
        });

        long size = 0;
        if (nrtDirs != null) {
            for (File f : nrtDirs){
                size += FileUtils.sizeOf(f);
                FileUtils.deleteQuietly(f);
            }
        }

        return size;
    }

    @CheckForNull
    private LocalIndexDir findMatchingIndexDir(File dir) throws IOException {
        //Resolve to canonical file so that equals can work reliable
        dir = dir.getCanonicalFile();

        Map<String, List<LocalIndexDir>> mapping = getIndexesPerPath();
        for (Map.Entry<String, List<LocalIndexDir>> e : mapping.entrySet()){
            for (LocalIndexDir idxDir : e.getValue()){
                if (idxDir.dir.equals(dir)){
                    return idxDir;
                }
            }
        }
        return null;
    }

    private long deleteOldFormatDir(String jcrPath) {
        File oldDir = getOldFormatDir(jcrPath);
        if (oldDir.exists()){
            long size = FileUtils.sizeOf(oldDir);
            if (!FileUtils.deleteQuietly(oldDir)){
                log.warn("Not able to deleted unused local index directory [{}]", oldDir.getAbsolutePath());
            } else {
                return size;
            }
        }
        return 0;
    }

    private File getOldFormatDir(String indexPath) {
        String subDir = getPathHash(indexPath);
        return new File(indexRootDir, subDir);
    }

    static String getFSSafeName(String e) {
        //TODO Exclude -_ like chars via [^\W_]
        return e.replaceAll("\\W", "");
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
