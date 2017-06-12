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

package org.apache.jackrabbit.oak.management;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonMap;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.failed;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.fromCompositeData;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.succeeded;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.toTabularData;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.unavailable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.jmx.FileStoreBackupRestoreMBean;
import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean;
import org.apache.jackrabbit.oak.api.jmx.SessionMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.index.property.jmx.PropertyIndexAsyncReindexMBean;
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
public class RepositoryManager extends AnnotatedStandardMBean implements RepositoryManagementMBean {
    private final Whiteboard whiteboard;

    public RepositoryManager(@Nonnull Whiteboard whiteboard) {
        super(RepositoryManagementMBean.class);
        this.whiteboard = checkNotNull(whiteboard);
    }

    public String getName() {
        return "repository manager";
    }

    private <T> Status execute(Class<T> serviceType, Function<T, Status> operation) {
        return execute(serviceType, operation, Collections.emptyMap());
    }

    private <T> Status execute(Class<T> serviceType, Function<T, Status> operation, Map<String, String> filter) {
        Tracker<T> tracker;
        if (filter.isEmpty()) {
            tracker = whiteboard.track(serviceType);
        } else {
            tracker = whiteboard.track(serviceType, filter);
        }
        try {
            List<T> services = tracker.getServices();
            if (services.size() == 1) {
                return operation.apply(services.get(0));
            } else if (services.isEmpty()) {
                return unavailable("Cannot perform operation: no service of type " +
                        serviceType.getSimpleName() + " found."
                );
            } else {
                return failed("Cannot perform operation: multiple services of type " +
                        serviceType.getSimpleName() + " found."
                );
            }
        } finally {
            tracker.stop();
        }
    }

    private <T> Iterable<Status> executeAll(Class<T> serviceType, Function<T, Status> operation) {
        Tracker<T> tracker = whiteboard.track(serviceType);
        List<Status> statuses = newArrayList();
        try {
            for (T service : tracker.getServices()) {
                statuses.add(operation.apply(service));
            }
            return statuses;
        } finally {
            tracker.stop();
        }
    }

    @Override
    public CompositeData startBackup() {
        return execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(FileStoreBackupRestoreMBean fileStoreBackupRestoreMBean) {
                return fromCompositeData(fileStoreBackupRestoreMBean.startBackup());
            }
        }).toCompositeData();
    }

    @Override
    public CompositeData getBackupStatus() {
        return execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(FileStoreBackupRestoreMBean backupService) {
                return fromCompositeData(backupService.getBackupStatus());
            }
        }).toCompositeData();
    }

    @Override
    public CompositeData startRestore() {
        return execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(FileStoreBackupRestoreMBean backupService) {
                return fromCompositeData(backupService.startRestore());
            }
        }).toCompositeData();
    }

    @Override
    public CompositeData getRestoreStatus() {
        return execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(FileStoreBackupRestoreMBean backupService) {
                return fromCompositeData(backupService.getRestoreStatus());
            }
        }).toCompositeData();
    }

    @Override
    public CompositeData startDataStoreGC(final boolean markOnly) {
        return execute(BlobGCMBean.class, new Function<BlobGCMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(BlobGCMBean blobGCService) {
                return fromCompositeData(blobGCService.startBlobGC(markOnly));
            }
        }).toCompositeData();
    }

    @Override
    public CompositeData getDataStoreGCStatus() {
        return execute(BlobGCMBean.class, new Function<BlobGCMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(BlobGCMBean blobGCService) {
                return fromCompositeData(blobGCService.getBlobGCStatus());
            }
        }).toCompositeData();
    }

    @Override
    public CompositeData startRevisionGC() {
        return startRevisionGCForRole(null);
    }

    @Override
    public CompositeData startRevisionGCForRole(String role) {
        return execute(RevisionGCMBean.class, new Function<RevisionGCMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(RevisionGCMBean revisionGCService) {
                return fromCompositeData(revisionGCService.startRevisionGC());
            }
        }, singletonMap("role", role)).toCompositeData();
    }

    @Nonnull
    @Override
    public CompositeData cancelRevisionGC() {
        return cancelRevisionGCForRole(null);
    }

    @Nonnull
    @Override
    public CompositeData cancelRevisionGCForRole(String role) {
        return execute(RevisionGCMBean.class, new Function<RevisionGCMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(RevisionGCMBean revisionGCService) {
                return fromCompositeData(revisionGCService.cancelRevisionGC());
            }
        }, singletonMap("role", role)).toCompositeData();
    }

    @Override
    public CompositeData getRevisionGCStatus() {
        return getRevisionGCStatusForRole(null);
    }

    @Nonnull
    @Override
    public CompositeData getRevisionGCStatusForRole(String role) {
        return execute(RevisionGCMBean.class, new Function<RevisionGCMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(RevisionGCMBean revisionGCService) {
                return fromCompositeData(revisionGCService.getRevisionGCStatus());
            }
        }, singletonMap("role", role)).toCompositeData();
    }

    @Override
    public String checkpoint(final long lifetime) {
        Status status = execute(FileStoreBackupRestoreMBean.class, new Function<FileStoreBackupRestoreMBean, Status>() {
            @Nonnull
            @Override
            public Status apply(FileStoreBackupRestoreMBean backupService) {
                String checkpoint = backupService.checkpoint(lifetime);
                return succeeded(checkpoint);
            }
        });

        return status.isSuccess()
            ? status.getMessage()
            : null;
    }

    @Override
    public CompositeData startPropertyIndexAsyncReindex() {
        return execute(PropertyIndexAsyncReindexMBean.class,
                new Function<PropertyIndexAsyncReindexMBean, Status>() {
                    @Nonnull
                    @Override
                    public Status apply(PropertyIndexAsyncReindexMBean reindexer) {
                        return fromCompositeData(reindexer
                                .startPropertyIndexAsyncReindex());
                    }
                }).toCompositeData();
    }

    @Override
    public CompositeData getPropertyIndexAsyncReindexStatus() {
        return execute(PropertyIndexAsyncReindexMBean.class,
                new Function<PropertyIndexAsyncReindexMBean, Status>() {
                    @Nonnull
                    @Override
                    public Status apply(PropertyIndexAsyncReindexMBean reindexer) {
                        return fromCompositeData(reindexer
                                .getPropertyIndexAsyncReindexStatus());
                    }
                }).toCompositeData();
    }

    @Override
    public TabularData refreshAllSessions() {
        return toTabularData(executeAll(SessionMBean.class, new Function<SessionMBean, Status>() {
            @Override
            public Status apply(SessionMBean sessionMBean) {
                sessionMBean.refresh();
                return succeeded("OK");
            }
        }));
    }
}
