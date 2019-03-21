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

package org.apache.jackrabbit.oak.segment;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

public interface SegmentNodeStoreStatsMBean {
    String TYPE = "SegmentStoreStats";
    
    /**
     * @return  time series of the number of commits
     */
    CompositeData getCommitsCount();
    
    /**
     * @return  time series of the number of commits queuing
     */
    CompositeData getQueuingCommitsCount();
    
    /**
     * @return  time series of the commit times
     */
    CompositeData getCommitTimes();
    
    /**
     * @return  time series of the queuing times
     */
    CompositeData getQueuingTimes();
    
    /**
     * @return tabular data of the form &lt;commits,writerGroup&gt; collected 
     *         <b>in the last minute</b>
     * @throws OpenDataException if data is not available
     */
    TabularData getCommitsCountPerWriterGroupLastMinute() throws OpenDataException;
    
    /**
     * @return tabular data of the form &lt;commits,writer&gt; for writers 
     *         not included in groups
     * @throws OpenDataException if data is not available
     */
    TabularData getCommitsCountForOtherWriters() throws OpenDataException;
    
    /**
     * @return tabular data of the form <em>&lt;writer, writerDetails, queued, dequed, applied&gt;</em>
     * for each writer currently in the queue
     * @throws OpenDataException if data is not available
     */
    TabularData getQueuedWriters() throws OpenDataException;

    /**
     * @return data of the form <em>&lt;writer, writerDetails, queued, dequed, applied&gt;</em>
     * for the writer currently in committing. @{@code null} if none.
     * @throws OpenDataException if data is not available
     */
    CompositeData getCurrentWriter() throws OpenDataException;

    /**
     * Turns on/off, depending on the value of {@code flag}, the collection of
     * stack traces for each writer.
     * @param flag {@code boolean} indicating whether to collect or not
     */
    void setCollectStackTraces(boolean flag);

    /**
     * @return collectStackTraces status flag
     */
    boolean isCollectStackTraces();

    /**
     * Modifies the maximum number of writers outside already defined
     * groups to be recorded.
     * Changing the default value will reset the overall collection process.
     *
     * @param otherWritersLimit the new size
     */
    void setNumberOfOtherWritersToDetail(int otherWritersLimit);

    /**
     * @return maximum number of writers outside already defined
     * groups to be recorded
     */
    int getNumberOfOtherWritersToDetail();

    /**
     * @return current groups used for grouping writers.
     */
    String[] getWriterGroupsForLastMinuteCounts();

    /**
     * Modifies the groups used for grouping writers.
     * Changing the default value will reset the overall collection process.
     * @param writerGroups groups defined by regexps
     */
    void setWriterGroupsForLastMinuteCounts(String[] writerGroups);
}
