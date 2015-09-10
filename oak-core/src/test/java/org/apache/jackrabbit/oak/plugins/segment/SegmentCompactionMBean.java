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
     * Set the core pool size of the scheduler used to execute concurrent
     * operations.
     * @param corePoolSize
     */
    void setCorePoolSize(int corePoolSize);

    /**
     * @return the core pool size of the scheduler used to execute concurrent
     * operations.
     */
    int getCorePoolSize();

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
     * Determine whether to compaction should run exclusively wrt. concurrent writers.
     * @param value  run compaction exclusively iff {@code true}
     */
    void setUseCompactionLock(boolean value);

    /**
     * @return  Compaction runs exclusively wrt. concurrent writers iff {@code true}
     */
    boolean getUseCompactionLock();

    /**
     * Time to wait for the commit lock for committing the compacted head.
     * @param seconds  number of seconds to wait
     * @see SegmentNodeStore#locked(java.util.concurrent.Callable, long, java.util.concurrent.TimeUnit)
     */
    void setLockWaitTime(int seconds);

    /**
     * Time to wait for the commit lock for committing the compacted head.
     * @return  number of seconds
     * @see SegmentNodeStore#locked(java.util.concurrent.Callable, long, java.util.concurrent.TimeUnit)
     */
    int getLockWaitTime();

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
     * Set the maximal size of string properties
     * @param size  size in bytes
     */
    void setMaxStringSize(int size);

    /**
     * @return  maximal size of string properties in bytes
     */
    int getMaxStringSize();

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
     * Maximal number of write operations per scheduled writer
     * @param count  maximal number of operations
     */
    void setMaxWriteOps(int count);

    /**
     * @return  maximal number of operations
     */
    int getMaxWriteOps();

    /**
     * Set the maximal number of child node of a node
     * @param count  maximal number of child nodes
     */
    void setMaxNodeCount(int count);

    /**
     * @return  Maximal number of child nodes of a node
     */
    int getMaxNodeCount();

    /**
     * Set the maximal number of properties of a node
     * @param count  maximal number of properties
     */
    void setMaxPropertyCount(int count);

    /**
     * @return  Maximal number of properties of a node
     */
    int getMaxPropertyCount();

    /**
     * Set the ration of remove node operations wrt. all other operations.
     * @param ratio  ratio of node remove operations
     */
    void setNodeRemoveRatio(int ratio);

    /**
     * @return  Ratio of node remove operations
     */
    int getNodeRemoveRatio();

    /**
     * Set the ration of remove property operations wrt. all other operations.
     * @param ratio  ratio of property remove operations
     */
    void setPropertyRemoveRatio(int ratio);

    /**
     * @return  Ratio of property remove operations
     */
    int getPropertyRemoveRatio();

    /**
     * Set the ration of add node operations wrt. all other operations.
     * @param ratio  ratio of node add operations
     */
    void setNodeAddRatio(int ratio);

    /**
     * @return  Ratio of node add operations
     */
    int getNodeAddRatio();

    /**
     * Set the ration of add string property operations wrt. all other operations.
     * @param ratio  ratio of string property add operations
     */
    void setAddStringRatio(int ratio);

    /**
     * @return  Ratio of string property add operations
     */
    int getAddStringRatio();

    /**
     * Set the ration of add binary property operations wrt. all other operations.
     * @param ratio  ratio of binary property add operations
     */
    void setAddBinaryRatio(int ratio);

    /**
     * @return  Ratio of binary property add operations
     */
    int getAddBinaryRatio();

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
     * Determine whether the compaction map is persisted or in memory
     * @return  {@code true} if persisted, {@code false} otherwise
     */
    boolean getPersistCompactionMap();

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
     * @return  number of record referenced by the keys in this map.
     */
    long getRecordCount();

    /**
     * @return  number of segments referenced by the keys in this map.
     */
    long getSegmentCount();

    /**
     * @return  current depth of the compaction map
     */
    int getCompactionMapDepth();

    /**
     * @return  last error
     */
    String getLastError();
}
