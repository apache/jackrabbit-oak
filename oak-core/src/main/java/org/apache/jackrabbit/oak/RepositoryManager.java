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

package org.apache.jackrabbit.oak;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean;
import org.apache.jackrabbit.oak.plugins.backup.FileStoreBackupRestoreMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * Default implementation of the {@link RepositoryManagementMBean} based
 * on a {@link Whiteboard} instance, which is used to look up individual
 * service providers for backup ({@link FileStoreBackupRestoreMBean}), data store
 * garbage collections ({@link BlobGCMBean}) and revision store garbage
 * collections ({@link RevisionGCMBean}).
 */
public class RepositoryManager implements RepositoryManagementMBean {
    private final Whiteboard whiteboard;

    public RepositoryManager(@Nonnull Whiteboard whiteboard) {
        this.whiteboard = checkNotNull(whiteboard);
    }

    public String getName() {
        return "repository manager";
    }

    private <T> String execute(Class<T> serviceType, Function<T, String> operation) {
        Tracker<T> tracker = whiteboard.track(serviceType);
        try {
            List<T> services = tracker.getServices();
            if (services.size() == 1) {
                return operation.apply(services.get(0));
            } else if (services.isEmpty()) {
                return "Cannot perform operation: no service of type " +
                        serviceType.getSimpleName() + " found.";
            } else {
                return "Cannot perform operation: multiple services of type " +
                        serviceType.getSimpleName() + " found.";
            }
        } finally {
            tracker.stop();
        }
    }

    @Override
    public String startBackup() {
        return execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, String>() {
            @Nullable
            @Override
            public String apply(FileStoreBackupRestoreMBean fileStoreBackupRestoreMBean) {
                return fileStoreBackupRestoreMBean.startBackup();
            }
        });
    }

    @Override
    public String getBackupStatus() {
        return execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, String>() {
            @Nullable
            @Override
            public String apply(FileStoreBackupRestoreMBean backupService) {
                return backupService.getBackupStatus();
            }
        });
    }

    @Override
    public String startRestore() {
        return execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, String>() {
            @Nullable
            @Override
            public String apply(FileStoreBackupRestoreMBean backupService) {
                return backupService.startRestore();
            }
        });
    }

    @Override
    public String getRestoreStatus() {
        return execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, String>() {
            @Nullable
            @Override
            public String apply(FileStoreBackupRestoreMBean backupService) {
                return backupService.getRestoreStatus();
            }
        });
    }

    @Override
    public String startDataStoreGC() {
        return execute(BlobGCMBean.class, new Function<BlobGCMBean, String>() {
            @Nullable
            @Override
            public String apply(BlobGCMBean blobGCService) {
                return blobGCService.startBlobGC();
            }
        });
    }

    @Override
    public String getDataStoreGCStatus() {
        return execute(BlobGCMBean.class, new Function<BlobGCMBean, String>() {
            @Nullable
            @Override
            public String apply(BlobGCMBean blobGCService) {
                return blobGCService.getBlobGCStatus();
            }
        });
    }

    @Override
    public String startRevisionGC() {
        return execute(RevisionGCMBean.class, new Function<RevisionGCMBean, String>() {
            @Nullable
            @Override
            public String apply(RevisionGCMBean revisionGCService) {
                return revisionGCService.startRevisionGC();
            }
        });
    }

    @Override
    public String getRevisionGCStatus() {
        return execute(RevisionGCMBean.class, new Function<RevisionGCMBean, String>() {
            @Nullable
            @Override
            public String apply(RevisionGCMBean revisionGCService) {
                return revisionGCService.getRevisionGCStatus();
            }
        });
    }

}
