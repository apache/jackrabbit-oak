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

import java.io.IOException;

import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks that all files in local which are present in remote have same file length.
 * If there is a size mismatch in any one of the file then whole of local index content
 * would be purged
 */
public class IndexSanityChecker {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Directory local;
    private final Directory remote;
    private final String indexPath;
    private int localFileCount;
    private int remoteFileCount;
    private long localDirSize;
    private long remoteDirSize;

    public IndexSanityChecker(String indexPath, Directory local, Directory remote) {
        this.local = local;
        this.remote = remote;
        this.indexPath = indexPath;
    }

    public boolean check() throws IOException {
        boolean allFine = true;
        //TODO Add support for checksum based checks
        if (isThereASizeMismatch()){
            //In case of any mismatch just purge all local files
            deleteAllFiles(local);
            allFine = false;
        } else {
            //Remove local files which are not found in remote
            for (String fileName : local.listAll()) {
                if (!remote.fileExists(fileName)) {
                    local.deleteFile(fileName);
                }
            }
        }

        if (allFine) {
            log.info("Local index directory content found to be valid for index [{}]. " +
                    "Stats Local: {} files ({}), Remote: {} files ({})", indexPath,
                    localFileCount, IOUtils.humanReadableByteCount(localDirSize),
                    remoteFileCount, IOUtils.humanReadableByteCount(remoteDirSize));
        } else {
            log.warn("Local index directory content were not found to be in sync with remote for index [{}]. " +
                    "Local directory content has been purged and would be synced again from remote", indexPath);
        }
        return allFine;
    }

    private boolean isThereASizeMismatch() throws IOException {
        for (String fileName : remote.listAll()){
            long localLength = DirectoryUtils.getFileLength(local, fileName);
            long remoteLength = remote.fileLength(fileName);

            //This is a weak check based on length.
            if (localLength > 0 && localLength != remoteLength){
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
        for (String fileName : dir.listAll()){
            dir.deleteFile(fileName);
        }
    }
}
