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

package org.apache.jackrabbit.oak.plugins.blob;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Name;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

/**
 * MBean for starting and monitoring the progress of
 * blob garbage collection.
 *
 * @see org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean
 */
public interface BlobGCMBean {
    String TYPE = "BlobGarbageCollection";

    /**
     * Initiate a data store garbage collection operation.
     *
     * @param markOnly whether to only mark references and not sweep in the mark and sweep operation.
     * @return  the status of the operation right after it was initiated
     */
    CompositeData startBlobGC(@Name("markOnly")
            @Description("Set to true to only mark references and not sweep in the mark and sweep operation. " +
                     "This mode is to be used when the underlying BlobStore is shared between multiple " +
                     "different repositories. For all other cases set it to false to perform full garbage collection")
                                boolean markOnly);

    /**
     * Initiate a data store garbage collection operation.
     *
     * @param markOnly whether to only mark references and not sweep in the mark and sweep operation.
     * @param forceBlobIdRetrieve whether to force retrieve blob ids from datastore
     * @return  the status of the operation right after it was initiated
     */
    CompositeData startBlobGC(@Name("markOnly")
    @Description("Set to true to only mark references and not sweep in the mark and sweep operation. " +
        "This mode is to be used when the underlying BlobStore is shared between multiple " +
        "different repositories. For all other cases set it to false to perform full garbage collection")
        boolean markOnly, @Name("forceBlobIdRetrieve")
    @Description("Set to true to force retrieve all ids from the datastore bypassing any local tracking")
        boolean forceBlobIdRetrieve);

    /**
     * Data store garbage collection status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull
    CompositeData getBlobGCStatus();
    
    /**
     * Show details of the data Store garbage collection process.
     * 
     * @return List of available repositories and their status
     */
    TabularData getGlobalMarkStats();
    
    /**
     * Data Store consistency check
     * 
     * @return the missing blobs
     */
    CompositeData checkConsistency();
    
    /**
     * Consistency check status
     * 
     * @return the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull 
    CompositeData getConsistencyCheckStatus();
}
