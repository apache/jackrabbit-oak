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
import static org.apache.jackrabbit.oak.plugins.backup.FileStoreBackup.backup;
import static org.apache.jackrabbit.oak.plugins.backup.FileStoreRestore.restore;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.management.ManagementOperation;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link FileStoreBackupRestoreMBean} based on a file.
 */
public class FileStoreBackupRestore implements FileStoreBackupRestoreMBean {
    private static final Logger log = LoggerFactory.getLogger(FileStoreBackupRestore.class);

    public static final String BACKUP_OP_NAME = "Backup";
    public static final String RESTORE_OP_NAME = "Restore";

    private final NodeStore store;
    private final File file;
    private final Executor executor;

    private ManagementOperation backupOp = done(BACKUP_OP_NAME, 0);
    private ManagementOperation restoreOp = done(RESTORE_OP_NAME, 0);

    /**
     * @param store  store to back up from or restore to
     * @param file   file to back up to or restore from
     * @param executor  executor for running the back up or restore operation
     */
    public FileStoreBackupRestore(
            @Nonnull NodeStore store,
            @Nonnull File file,
            @Nonnull Executor executor) {
        this.store = checkNotNull(store);
        this.file = checkNotNull(file);
        this.executor = checkNotNull(executor);
    }

    @Override
    public synchronized CompositeData startBackup() {
        if (backupOp.isDone()) {
            backupOp = new ManagementOperation("Backup", new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long t0 = nanoTime();
                    backup(store, file);
                    return nanoTime() - t0;
                }
            });
            executor.execute(backupOp);
        }
        return getBackupStatus();
    }

    @Override
    public synchronized CompositeData getBackupStatus() {
        return backupOp.getStatus().toCompositeData();
    }

    @Override
    public synchronized CompositeData startRestore() {
        if (restoreOp.isDone()) {
            restoreOp = new ManagementOperation("Restore", new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long t0 = nanoTime();
                    restore(file, store);
                    return nanoTime() - t0;
                }
            });
            executor.execute(restoreOp);
        }
        return getRestoreStatus();
    }

    @Override
    public synchronized CompositeData getRestoreStatus() {
        return restoreOp.getStatus().toCompositeData();
    }

    @Override
    public String checkpoint(long lifetime) {
        return store.checkpoint(lifetime);
    }

}
