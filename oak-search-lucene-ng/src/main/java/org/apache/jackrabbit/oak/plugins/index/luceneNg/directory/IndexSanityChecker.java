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

package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks that all files in local which are present in remote have same file length.
 * If there is a size mismatch in any one of the file then whole of local index content
 * would be purged.
 * <p>
 * Port of {@code oak-lucene}'s {@code IndexSanityChecker} for Lucene 9's {@link Directory}
 * API. {@code remote} is narrowed to {@link OakDirectory} (this module's only remote
 * implementation) and a {@code localDir} {@link File} is threaded through so local-side
 * existence/length checks can use direct {@link File} calls (via
 * {@link LuceneNgIndexCopier#existsLocally(File, String)}/{@link LuceneNgIndexCopier#localFileLength(File, String)})
 * instead of the removed {@code Directory.fileExists} and legacy's {@code DirectoryUtils}.
 */
public class IndexSanityChecker {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Directory local;
    private final File localDir;
    private final OakDirectory remote;
    private final String indexPath;
    private int localFileCount;
    private int remoteFileCount;
    private long localDirSize;
    private long remoteDirSize;

    public IndexSanityChecker(String indexPath, Directory local, File localDir, OakDirectory remote) {
        this.local = local;
        this.localDir = localDir;
        this.remote = remote;
        this.indexPath = indexPath;
    }

    public boolean check(IndexSanityStatistics stats) throws IOException {
        boolean allFine = true;
        long start = System.currentTimeMillis();
        //TODO Add support for checksum based checks
        if (isThereASizeMismatch()) {
            //In case of any mismatch just purge all local files
            deleteAllFiles(local);
            allFine = false;
        } else {
            //Remove local files which are not found in remote. Hoist the listAll() call and
            //Set build above the loop - this method runs once per index-generation open (not
            //per query), but a per-iteration remote.listAll() scan would still be an
            //accidental O(n^2) worth avoiding.
            Set<String> remoteFiles = SetUtils.toLinkedSet(remote.listAll());
            for (String fileName : local.listAll()) {
                if (!remoteFiles.contains(fileName)) {
                    local.deleteFile(fileName);
                }
            }
        }
        stats.addDuration(System.currentTimeMillis() - start);
        stats.addIndexSize(localDirSize);

        if (allFine) {
            log.info("Local index directory content found to be valid for index [{}]. " +
                    "Stats Local: {} files ({}), Remote: {} files ({}), accumulated statistics on checking index sanity: {}", indexPath,
                    localFileCount, IOUtils.humanReadableByteCount(localDirSize),
                    remoteFileCount, IOUtils.humanReadableByteCount(remoteDirSize),
                    stats.toString());
        } else {
            log.warn("Local index directory content were not found to be in sync with remote for index [{}]. " +
                    "Local directory content has been purged and would be synced again from remote.", indexPath);
        }
        return allFine;
    }

    private boolean isThereASizeMismatch() throws IOException {
        for (String fileName : remote.listAll()) {
            long localLength = LuceneNgIndexCopier.localFileLength(localDir, fileName);
            long remoteLength = 0;
            try {
                /*
                Remote file may not be present when we ask for file length after retrieving remote file list.
                 */
                remoteLength = remote.fileLength(fileName);
            } catch (FileNotFoundException ignore) {
              /*
              localFileLength returns -1 in case local file is not present
               */
                if (localLength == -1) {
                    log.info("{} is not present on remote as well as local", fileName);
                    continue;
                }
            }

            //This is a weak check based on length.
            if (localLength > 0 && localLength != remoteLength) {
                log.warn("[{}] Found local copy for {} in {} but size of local {} differs from remote {}. ",
                        indexPath, fileName, local, localLength, remoteLength);
                return true;
            }

            if (localLength > 0) {
                localDirSize += localLength;
                localFileCount++;
            }

            remoteDirSize += remoteLength;
            remoteFileCount++;
        }
        return false;
    }

    private static void deleteAllFiles(Directory dir) throws IOException {
        for (String fileName : dir.listAll()) {
            dir.deleteFile(fileName);
        }
    }


    /**
     * Accumulated statistics across multiple invocations of the IndexSanityChecker
     *
     */
    public static class IndexSanityStatistics {

        long totalDurationInMs;
        long totalIndexSize;

        /**
         * Record the time spend to check the sanity of indexes
         * @param milis the time in miliseconds
         */
        public void addDuration(long milis) {
            totalDurationInMs += milis;
        }

        /**
         * Return the accumulated time for checking the sanity of indexes
         * @return the total duration in miliseconds
         */
        public long getAccumulatedDuration() {
            return totalDurationInMs;
        }

        /**
         * Record the size of an index considered
         * @param bytes
         */
        public void addIndexSize(long bytes) {
            totalIndexSize += bytes;
        }

        /**
         * Return the accumulated index size
         * @return index size in bytes
         */
        public long getAccumulatedIndexSize() {
            return totalIndexSize;
        }

        @Override
        public String toString() {
            return String.format("[duration: %d ms, "
                    + "checked index size: %d bytes (%s)]", totalDurationInMs,
                    totalIndexSize, IOUtils.humanReadableByteCount(totalIndexSize));
        }
    }
}
