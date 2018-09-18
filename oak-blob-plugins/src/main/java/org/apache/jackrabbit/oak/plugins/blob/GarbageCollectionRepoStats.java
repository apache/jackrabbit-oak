/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob;

/**
 * Garbage collection stats for the repository.
 */
public class GarbageCollectionRepoStats {
    private String repositoryId;

    private boolean local;
    
    private long startTime;
    
    private long endTime;
    
    private long length;
    
    private int numLines;
    
    public String getRepositoryId() {
        return repositoryId;
    }
    
    public void setRepositoryId(String repositoryId) {
        this.repositoryId = repositoryId;
    }
    
    public long getEndTime() {
        return endTime;
    }
    
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }
    
    public long getLength() {
        return length;
    }
    
    public void setLength(long length) {
        this.length = length;
    }
    
    public void setNumLines(int numLines) {
        this.numLines = numLines;
    }
    
    public int getNumLines() {
        return numLines;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }
}
