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

import org.apache.commons.io.FileUtils;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory.INDEX_METADATA_FILE_NAME;

public final class LocalIndexDir implements Comparable<LocalIndexDir> {
    final File dir;
    final IndexMeta indexMeta;

    public LocalIndexDir(File dir) throws IOException {
        this.dir = dir.getCanonicalFile();
        File indexDetails = new File(dir, IndexRootDirectory.INDEX_METADATA_FILE_NAME);
        checkState(isIndexDir(dir), "No file [%s] found in dir [%s]",
                INDEX_METADATA_FILE_NAME, dir.getAbsolutePath());
        this.indexMeta = new IndexMeta(indexDetails);
    }

    public long size() {
        return FileUtils.sizeOfDirectory(dir);
    }

    public boolean isEmpty() {
        String[] listing = dir.list();

        //If some IO error occurs listing would be null
        //In such a case better to return false
        if (listing == null) {
            return false;
        }

        //If the dir only has the meta file then it would be
        //considered as empty
        return listing.length == 1;
    }

    public String getJcrPath() {
        return indexMeta.indexPath;
    }

    public String getFSPath() {
        return dir.getAbsolutePath();
    }

    @Override
    public int compareTo(LocalIndexDir o) {
        return indexMeta.compareTo(o.indexMeta);
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", dir.getAbsolutePath(), indexMeta);
    }

    static boolean isIndexDir(File file){
        File indexDetails = new File(file, IndexRootDirectory.INDEX_METADATA_FILE_NAME);
        return indexDetails.exists();
    }
}