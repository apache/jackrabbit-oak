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

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link FileStoreBackupRestoreMBean} based on a file.
 */
public class FileStoreBackupRestore implements FileStoreBackupRestoreMBean {

    private static final Logger log = LoggerFactory.getLogger(FileStoreBackupRestore.class);

    private final NodeStore store;
    private final File file;
    private final ExecutorService executorService;

    private Future<Long> backupOp;
    private long backupEndTime;
    private Future<Long> restoreOp;
    private long restoreEndTime;

    /**
     * @param store  store to back up from or restore to
     * @param file   file to back up to or restore from
     * @param executorService  executor service for running the back up or restore operation
     *                         in the background.
     */
    public FileStoreBackupRestore(
            @Nonnull NodeStore store,
            @Nonnull File file,
            @Nonnull ExecutorService executorService) {
        this.store = checkNotNull(store);
        this.file = checkNotNull(file);
        this.executorService = checkNotNull(executorService);
    }

    @Override
    public synchronized String startBackup() {
        if (backupOp != null && !backupOp.isDone()) {
            return "Backup already running";
        } else {
            backupOp = executorService.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long t0 = System.nanoTime();
                    FileStoreBackup.backup(store, file);
                    return System.nanoTime() - t0;
                }
            });
            return getBackupStatus();
        }
    }

    @Override
    public synchronized String getBackupStatus() {
        if (backupOp == null) {
            return "Backup not started";
        } else if (backupOp.isCancelled()) {
            return "Backup cancelled";
        } else if (backupOp.isDone()) {
            try {
                return "Backup completed in " + formatTime(backupOp.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "Backup status unknown: " + e.getMessage();
            } catch (ExecutionException e) {
                log.error("Backup failed", e.getCause());
                return "Backup failed: " + e.getCause().getMessage();
            }
        } else {
            return "Backup running";
        }
    }

    @Override
    public synchronized String startRestore() {
        if (restoreOp != null && !restoreOp.isDone()) {
            return "Restore already running";
        } else {
            restoreOp = executorService.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long t0 = System.nanoTime();
                    FileStoreRestore.restore(file, store);
                    return System.nanoTime() - t0;
                }
            });
            return getRestoreStatus();
        }
    }

    @Override
    public synchronized String getRestoreStatus() {
        if (restoreOp == null) {
            return "Restore not started";
        } else if (restoreOp.isCancelled()) {
            return "Restore cancelled";
        } else if (restoreOp.isDone()) {
            try {
                return "Restore completed in " + formatTime(restoreOp.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "Restore status unknown: " + e.getMessage();
            } catch (ExecutionException e) {
                log.error("Restore failed", e.getCause());
                return "Restore failed: " + e.getCause().getMessage();
            }
        } else {
            return "Restore running";
        }
    }

    private static String formatTime(long nanos) {
        return TimeUnit.MINUTES.convert(nanos, TimeUnit.NANOSECONDS) + " minutes";
    }

    @Override
    public String checkpoint(long lifetime) {
        return store.checkpoint(lifetime);
    }
}
