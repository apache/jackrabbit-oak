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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.osgi.annotation.versioning.ProviderType;

/**
 * This interface exposes repository management operations and the status
 * of such operations. This interface only provides high level functionality
 * for starting certain management operations and monitoring their outcomes.
 * Parametrisation and configuration of the operations is beyond the scope
 * of this interface and must be achieved by other means. For example
 * through a dedicated MBean of the specific service providing the
 * respective functionality. Furthermore not all operations might be
 * available in all deployments or at all times. However the status should
 * give a clear indication for this case.
 * <p>
 * The status of an operation is represented by a {@code CompositeData}
 * instance consisting at least of the items {@code code}, {@code id},
 * and {@code message}. Implementations are free to add further items.
 * <p>
 * The {@code code} item is an integer encoding the current status of
 * the respective operation. Valid values and its semantics are:
 * <ul>
 *     <li>{@code 0}: <em>Operation not available</em>. For example
 *     because the system does not implement the operation or the
 *     system is in a state where it does not allow the operation to
 *     be carried out (e.g. the operation is already running). The
 *     {@code message} should give further indication of the exact
 *     reason.</li>
 *     <li>{@code 1}: <em>Status not available</em>. Usually because
 *     there was no prior attempt to start the operation. The
 *     {@code message} should give further indication of the exact
 *     reason.</li>
 *     <li>{@code 2}: <em>Operation initiated</em>. The {@code message}
 *     should give further information of when the operation was
 *     initiated. This status mean that the operation will be performed
 *     some time in the future without impacting overall system behaviour
 *     and that no further status updates will be available until this
 *     operation is performed next time.</li>
 *     <li>{@code 3}: <em>Operation running</em>.</li>
 *     <li>{@code 4}: <em>Operation succeeded</em>. The {@code message} should
 *     give further information on how long the operation took to
 *     complete.</li>
 *     <li>{@code 5}: Operation failed. The {@code message} should give
 *     further information on the reason for the failure.</li>
 * </ul>
 * <p>
 * In all cases the {@code message} may provide additional information
 * that might be useful in the context of the operation.
 * <p>
 * The {@code id} is an identifier for the invocation of an operation.
 * It is reported as a part of the status for clients to relate the
 * status to invocation. {@code -1} is returned when not available.
 */
@ProviderType
public interface RepositoryManagementMBean {
    String TYPE = "RepositoryManagement";

    /**
     * Enum whose ordinals correspond to the status codes.
     */
    enum StatusCode {
        UNAVAILABLE("Operation not available"),
        NONE("Status not available"),
        INITIATED("Operation initiated"),
        RUNNING("Operation running"),
        SUCCEEDED("Operation succeeded"),
        FAILED("Operation failed");

        public final String name;

        StatusCode(String name) {
            this.name = name;
        }
    }

    /**
     * Initiate a backup operation.
     *
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    @Description("Creates a backup of the persistent state of the repository")
    CompositeData startBackup();

    /**
     * Backup status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull
    @Description("The status of the ongoing operation, or the terminal status of the last completed backup operation")
    CompositeData getBackupStatus();

    /**
     * Initiate a restore operation.
     *
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    @Description("Restores the repository from a backup")
    CompositeData startRestore();

    /**
     * Restore status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull
    @Description("The status of the ongoing operation, or the terminal status of the last completed restore operation")
    CompositeData getRestoreStatus();

    /**
     * Initiate a data store garbage collection operation
     *
     * @param markOnly whether to only mark references and not sweep in the mark and sweep operation.
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    @Description("Initiates a data store garbage collection operation")
    CompositeData startDataStoreGC(@Name("markOnly")
            @Description("Set to true to only mark references and not sweep in the mark and sweep operation. " +
                    "This mode is to be used when the underlying BlobStore is shared between multiple " +
                    "different repositories. For all other cases set it to false to perform full garbage collection")
                                   boolean markOnly);

    /**
     * Data store garbage collection status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull
    @Description("Data store garbage collection status")
    CompositeData getDataStoreGCStatus();

    /**
     * Initiate a revision garbage collection operation
     *
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    @Description("Initiates a revision garbage collection operation")
    CompositeData startRevisionGC();

    /**
     * Initiate a revision garbage collection operation
     *
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    @Description("Initiates a revision garbage collection operation for a given role")
    CompositeData startRevisionGCForRole(String role);

    /**
     * Cancel a running revision garbage collection operation. Does nothing
     * if revision garbage collection is not running.
     *
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    @Description("Cancel a running revision garbage collection operation. Does nothing if revision garbage collection is not running.")
    CompositeData cancelRevisionGC();

    /**
     * Cancel a running revision garbage collection operation for a given role.
     * Does nothing if revision garbage collection is not running.
     *
     * @return  the status of the operation right after it was initiated
     */
    @Nonnull
    @Description("Cancel a running revision garbage collection operation. Does nothing if revision garbage collection is not running.")
    CompositeData cancelRevisionGCForRole(String role);

    /**
     * Revision garbage collection status
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull
    @Description("Revision garbage collection status")
    CompositeData getRevisionGCStatus();

    /**
     * Revision garbage collection status for a given role.
     *
     * @return  the status of the ongoing operation or if none the terminal
     * status of the last operation or <em>Status not available</em> if none.
     */
    @Nonnull
    @Description("Revision garbage collection status for a given role")
    CompositeData getRevisionGCStatusForRole(String role);

    /**
     * Creates a new checkpoint of the latest root of the tree. The checkpoint
     * remains valid for at least as long as requested and allows that state
     * of the repository to be retrieved using the returned opaque string
     * reference.
     *
     * @param lifetime time (in milliseconds, &gt; 0) that the checkpoint
     *                 should remain available
     * @return string reference of this checkpoint or {@code null} if
     * the checkpoint could not be set.
     *
     * @deprecated Use {@link CheckpointMBean} instead
     */
    @Deprecated
    @CheckForNull
    String checkpoint(long lifetime);

    /**
     * Initiate a reindex operation for the property indexes marked for
     * reindexing
     * 
     * @return the status of the operation right after it was initiated
     */
    @Nonnull
    @Description("Initiates a reindex operation for the property indexes marked for reindexing")
    CompositeData startPropertyIndexAsyncReindex();

    /**
     * Asynchronous Property Index reindexing status
     * 
     * @return the status of the ongoing operation or if none the terminal
     *         status of the last operation or <em>Status not available</em> if
     *         none.
     */
    @Nonnull
    @Description("Asynchronous Property Index reindexing status")
    CompositeData getPropertyIndexAsyncReindexStatus();

    /**
     * Refresh all currently open sessions.
     * <em>Warning</em>: this operation might be disruptive to the owner of the affected sessions
     */
    @Nonnull
    @Description("Refresh all currently open sessions")
    TabularData refreshAllSessions();

}
