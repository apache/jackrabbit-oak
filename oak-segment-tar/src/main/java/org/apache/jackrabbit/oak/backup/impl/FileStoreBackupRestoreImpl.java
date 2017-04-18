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

package org.apache.jackrabbit.oak.backup.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.nanoTime;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.formatTime;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.done;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.newManagementOperation;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.api.jmx.FileStoreBackupRestoreMBean;
import org.apache.jackrabbit.oak.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.backup.FileStoreRestore;
import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentReader;

/**
 * Default implementation of {@link FileStoreBackupRestoreMBean} based on a
 * file.
 */
public class FileStoreBackupRestoreImpl implements FileStoreBackupRestoreMBean {

    private static final String BACKUP_OP_NAME = "Backup";

    private static final String RESTORE_OP_NAME = "Restore";

    private final SegmentNodeStore store;

    private final Revisions revisions;

    private final SegmentReader reader;

    private final File file;

    private final Executor executor;

    private ManagementOperation<String> backupOp = done(BACKUP_OP_NAME, "");

    private ManagementOperation<String> restoreOp = done(RESTORE_OP_NAME, "");

    private final FileStoreBackup fileStoreBackup;

    private final FileStoreRestore fileStoreRestore;

    /**
     * @param store    store to back up from or restore to
     * @param file     file to back up to or restore from
     * @param executor executor for running the back up or restore operation
     */
    public FileStoreBackupRestoreImpl(
            @Nonnull SegmentNodeStore store,
            @Nonnull Revisions revisions,
            @Nonnull SegmentReader reader,
            @Nonnull File file,
            @Nonnull Executor executor
    ) {
        this.store = checkNotNull(store);
        this.revisions = checkNotNull(revisions);
        this.reader = checkNotNull(reader);
        this.file = checkNotNull(file);
        this.executor = checkNotNull(executor);

        this.fileStoreBackup = new FileStoreBackupImpl();
        this.fileStoreRestore = new FileStoreRestoreImpl();
    }

    @Override
    @Nonnull
    public synchronized CompositeData startBackup() {
        if (backupOp.isDone()) {
            backupOp = newManagementOperation("Backup", new Callable<String>() {

                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    fileStoreBackup.backup(reader, revisions, file);
                    return "Backup completed in " + formatTime(nanoTime() - t0);
                }

            });
            executor.execute(backupOp);
        }

        return getBackupStatus();
    }

    @Override
    @Nonnull
    public synchronized CompositeData getBackupStatus() {
        return backupOp.getStatus().toCompositeData();
    }

    @Override
    @Nonnull
    public synchronized CompositeData startRestore() {
        if (restoreOp.isDone()) {
            restoreOp = newManagementOperation("Restore", new Callable<String>() {

                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    fileStoreRestore.restore(file);
                    return "Restore completed in " + formatTime(nanoTime() - t0);
                }

            });
            executor.execute(restoreOp);
        }

        return getRestoreStatus();
    }

    @Override
    @Nonnull
    public synchronized CompositeData getRestoreStatus() {
        return restoreOp.getStatus().toCompositeData();
    }

    @Override
    @Nonnull
    public String checkpoint(long lifetime) {
        return store.checkpoint(lifetime);
    }

}
