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

package org.apache.jackrabbit.oak.plugins.backup;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.nanoTime;
import static org.apache.jackrabbit.oak.management.ManagementOperation.done;
import static org.apache.jackrabbit.oak.management.ManagementOperation.newManagementOperation;
import static org.apache.jackrabbit.oak.management.ManagementOperation.Status.formatTime;
import static org.apache.jackrabbit.oak.plugins.backup.FileStoreBackup.backup;
import static org.apache.jackrabbit.oak.plugins.backup.FileStoreRestore.restore;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.api.jmx.FileStoreBackupRestoreMBean;
import org.apache.jackrabbit.oak.management.ManagementOperation;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Default implementation of {@link FileStoreBackupRestoreMBean} based on a file.
 */
@Deprecated
public class FileStoreBackupRestore implements FileStoreBackupRestoreMBean {

    @Deprecated
    public static final String BACKUP_OP_NAME = "Backup";

    @Deprecated
    public static final String RESTORE_OP_NAME = "Restore";

    private final NodeStore store;
    private final File file;
    private final Executor executor;

    private ManagementOperation<String> backupOp = done(BACKUP_OP_NAME, "");
    private ManagementOperation<String> restoreOp = done(RESTORE_OP_NAME, "");

    /**
     * @param store  store to back up from or restore to
     * @param file   file to back up to or restore from
     * @param executor  executor for running the back up or restore operation
     */
    @Deprecated
    public FileStoreBackupRestore(
            @Nonnull NodeStore store,
            @Nonnull File file,
            @Nonnull Executor executor) {
        this.store = checkNotNull(store);
        this.file = checkNotNull(file);
        this.executor = checkNotNull(executor);
    }

    @Override
    @Deprecated
    public synchronized CompositeData startBackup() {
        if (backupOp.isDone()) {
            backupOp = newManagementOperation("Backup", new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    backup(store, file);
                    return "Backup completed in " + formatTime(nanoTime() - t0);
                }
            });
            executor.execute(backupOp);
        }
        return getBackupStatus();
    }

    @Override
    @Deprecated
    public synchronized CompositeData getBackupStatus() {
        return backupOp.getStatus().toCompositeData();
    }

    @Override
    @Deprecated
    public synchronized CompositeData startRestore() {
        if (restoreOp.isDone()) {
            restoreOp = newManagementOperation("Restore", new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    restore(file, store);
                    return "Restore completed in " + formatTime(nanoTime() - t0);
                }
            });
            executor.execute(restoreOp);
        }
        return getRestoreStatus();
    }

    @Override
    @Deprecated
    public synchronized CompositeData getRestoreStatus() {
        return restoreOp.getStatus().toCompositeData();
    }

    @Override
    @Deprecated
    public String checkpoint(long lifetime) {
        return store.checkpoint(lifetime);
    }

}
