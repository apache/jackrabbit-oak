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

package org.apache.jackrabbit.oak.segment.file;

import javax.management.openmbean.CompositeData;

public interface FileStoreStatsMBean {

    String TYPE = "FileStoreStats";

    long getApproximateSize();

    /**
     * @return the number of tar files in the segment store
     */
    int getTarFileCount();

    /**
     * @return the number of segments in the segment store
     */
    int getSegmentCount();

    /**
     * @return time series of the writes to repository
     */
    CompositeData getWriteStats();

    /**
     * @return time series of the writes to repository
     */
    CompositeData getRepositorySize();

    String fileStoreInfoAsString();

    /**
     * @return count of the writes to journal
     */
    long getJournalWriteStatsAsCount();

    /**
     * @return time series of the writes to journal
     */
    CompositeData getJournalWriteStatsAsCompositeData();
}
