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

package org.apache.jackrabbit.oak.plugins.segment;

/**
 * MBean for monitoring and interacting with the {@link SegmentCompactionIT}
 * longevity test.
 */
public interface SegmentCompactionMBean {

    /**
     * Stop the test.
     */
    void stop();

    /**
     * Set the compaction interval
     * @param minutes  number of minutes to wait between compaction cycles.
     */
    void setCompactionInterval(int minutes);

    /**
     * @return  the compaction interval in minutes.
     */
    int getCompactionInterval();

    /**
     * @return  Time stamp from when compaction last ran.
     */
    String getLastCompaction();

    /**
     * Set the maximal number of concurrent readers
     * @param count
     */
    void setMaxReaders(int count);

    /**
     * @return  maximal number of concurrent readers
     */
    int getMaxReaders();

    /**
     * Set the maximal number of concurrent writers
     * @param count
     */
    void setMaxWriters(int count);

    /**
     * @return  maximal number of concurrent writers
     */
    int getMaxWriters();

    /**
     * Set the maximal size of the store
     * @param size  size in bytes
     */
    void setMaxStoreSize(long size);

    /**
     * @return  maximal size of the store in bytes
     */
    long getMaxStoreSize();

    /**
     * Set the maximal size of binary properties
     * @param size  size in bytes
     */
    void setMaxBlobSize(int size);

    /**
     * @return  maximal size of binary properties in bytes
     */
    int getMaxBlobSize();

    /**
     * Set the maximal number of held references
     * @param count  maximal number of references
     */
    void setMaxReferences(int count);

    /**
     * @return  maximal number of held references
     */
    int getMaxReferences();

    /**
     * Add a reference to the current root or release a held reference.
     * @param set  add a reference if {@code true}, otherwise release any held reference
     */
    void setRootReference(boolean set);

    /**
     * @return  {@code true} if currently a root reference is being held. {@code false} otherwise.
     */
    boolean getRootReference();

    /**
     * @return  actual number of concurrent readers
     */
    int getReaderCount();

    /**
     * @return  actual number of concurrent writers
     */
    int getWriterCount();

    /**
     * @return actual number of held references (not including any root reference)
     */
    int getReferenceCount();

    /**
     * @return  current size of the {@link org.apache.jackrabbit.oak.plugins.segment.file.FileStore}
     */
    long getFileStoreSize();

    /**
     * @return  current weight of the compaction map
     */
    long getCompactionMapWeight();

    /**
     * @return  current depth of the compaction map
     */
    int getCompactionMapDepth();

    /**
     * @return  last error
     */
    String getLastError();
}
