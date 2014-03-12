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

/**
 * This interface exposes repository management operations
 * and the status of such operations.
 * <p>
 * The status of an operation is an opaque string describing
 * in a human readable form what the operation currently does,
 * which might depend on the particular implementation performing
 * the operation. However the status status must always indicate
 * whether an operation is ongoing or terminated. In the latter case
 * it must indicate whether it terminated successfully or failed.
 * In all cases the status may provide additional information
 * like e.g. how far an ongoing operation progressed, what time
 * it took to complete a terminated operation, or information
 * about what caused a terminated operation to fail.
 */
public interface RepositoryManagementMBean {

    /**
     * Initiate a backup operation to a file at the given absolute path
     *
     * @param path  absolute path
     * @return  the status of the operation right after it was initiated
     */
    String startBackup(String path);

    /**
     * Backup status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or {@code null} if none.
     */
    String getBackupStatus();

    /**
     * Initiate a restore operation from a file at the given absolute path
     *
     * @param path  absolute path
     * @return  the status of the operation right after it was initiated
     */
    String startRestore(String path);

    /**
     * Restore status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or {@code null} if none.
     */
    String getRestoreStatus();

    /**
     * Initiate a data store garbage collection operation
     *
     * @return  the status of the operation right after it was initiated
     */
    String startDataStoreGC();

    /**
     * Data store garbage collection status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or {@code null} if none.
     */
    String getDataStoreGCStatus();

    /**
     * Initiate a revision garbage collection operation
     *
     * @return  the status of the operation right after it was initiated
     */
    String startRevisionGC();

    /**
     * Revision garbage collection status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or {@code null} if none.
     */
    String getRevisionGCStatus();
}
