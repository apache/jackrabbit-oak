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
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

public final class LocalIndexFile {
    final File dir;
    final String name;
    final long size;
    final boolean copyFromRemote;
    private volatile int deleteAttemptCount;
    final long creationTime = System.currentTimeMillis();

    public LocalIndexFile(Directory dir, String fileName,
                          long size, boolean copyFromRemote){
        this.copyFromRemote = copyFromRemote;
        this.dir = getFSDir(dir);
        this.name = fileName;
        this.size = size;
    }

    public LocalIndexFile(Directory dir, String fileName){
        this(dir, fileName, DirectoryUtils.getFileLength(dir, fileName), true);
    }

    public String getKey(){
        if (dir != null){
            return new File(dir, name).getAbsolutePath();
        }
        return name;
    }

    public boolean isCopyFromRemote() {
        return copyFromRemote;
    }

    public long getSize() {
        return size;
    }

    public void incrementAttemptToDelete(){
        deleteAttemptCount++;
    }

    public int getDeleteAttemptCount() {
        return deleteAttemptCount;
    }

    public String deleteLog(){
        return String.format("%s (%s, %d attempts, %d s)", name,
                humanReadableByteCount(size), deleteAttemptCount, timeTaken());
    }

    public String copyLog(){
        return String.format("%s (%s, %1.1f%%, %s, %d s)", name,
                humanReadableByteCount(actualSize()),
                copyProgress(),
                humanReadableByteCount(size), timeTaken());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalIndexFile localIndexFile = (LocalIndexFile) o;

        if (dir != null ? !dir.equals(localIndexFile.dir) : localIndexFile.dir != null)
            return false;
        return name.equals(localIndexFile.name);

    }

    @Override
    public int hashCode() {
        int result = dir != null ? dir.hashCode() : 0;
        result = 31 * result + name.hashCode();
        return result;
    }

    private long timeTaken(){
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - creationTime);
    }

    private float copyProgress(){
        return actualSize() * 1.0f / size * 100;
    }

    private long actualSize(){
        return dir != null ? new File(dir, name).length() : 0;
    }

    static File getFSDir(Directory dir) {
        if (dir instanceof FilterDirectory){
            dir = ((FilterDirectory) dir).getDelegate();
        }

        if (dir instanceof FSDirectory){
            return ((FSDirectory) dir).getDirectory();
        }

        return null;
    }
}
