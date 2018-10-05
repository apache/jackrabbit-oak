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
package org.apache.jackrabbit.oak.blob.cloud.s3.stats;

/**
 * MBean for JMX statistics pertaining to an S3DataStore.
 */
public interface S3DataStoreStatsMBean {
    String TYPE = "S3DataStoreStats";

    /**
     * Obtains the number of records that are in the process
     * of being "synced", meaning they are either scheduled to
     * be copied to S3 or are actively being copied to S3
     * but the copy of these files has not yet completed.
     *
     * @return number of syncs in progress (active).
     */
    long getActiveSyncs();

    /**
     * Determines whether a file-like entity with the given name
     * has been "synced" (completely copied) to S3.
     *
     * @param nodePathName - Path to the entity to check.  This is
     *                       the repository node path, not an external file path.
     * @return true if the file is synced to S3.
     */
    boolean isFileSynced(final String nodePathName);
}
