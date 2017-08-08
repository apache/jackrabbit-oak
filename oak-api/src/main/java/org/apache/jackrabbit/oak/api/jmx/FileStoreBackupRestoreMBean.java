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

package org.apache.jackrabbit.oak.api.jmx;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.osgi.annotation.versioning.ProviderType;

/**
 * MBean for backing up and restoring a {@code NodeStore}.
 *
 * @see org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean
 */
@ProviderType
public interface FileStoreBackupRestoreMBean {
    String TYPE = "FileStoreBackupRestore";
    
    /**
     * Initiate a backup operation.
     *
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    CompositeData startBackup();

    /**
     * Backup status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull
    CompositeData getBackupStatus();

    /**
     * Initiate a restore operation.
     *
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    CompositeData startRestore();

    /**
     * Restore status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull
    CompositeData getRestoreStatus();

    /**
     * Creates a new checkpoint of the latest root of the tree. The checkpoint
     * remains valid for at least as long as requested and allows that state
     * of the repository to be retrieved using the returned opaque string
     * reference.
     *
     * @param lifetime time (in milliseconds, &gt; 0) that the checkpoint
     *                 should remain available
     * @return string reference of this checkpoint
     *
     * @deprecated Use {@link org.apache.jackrabbit.oak.api.jmx.CheckpointMBean} instead
     */
    @Deprecated
    @Nonnull
    String checkpoint(long lifetime);

}
