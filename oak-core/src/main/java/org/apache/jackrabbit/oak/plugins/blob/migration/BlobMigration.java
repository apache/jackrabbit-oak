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

package org.apache.jackrabbit.oak.plugins.blob.migration;

import static java.lang.System.nanoTime;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.done;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.newManagementOperation;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.formatTime;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean.StatusCode;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class BlobMigration extends AnnotatedStandardMBean implements BlobMigrationMBean {

    public static final String OP_NAME = "Blob migration";

    private static final Logger log = LoggerFactory.getLogger(BlobMigrator.class);

    private ManagementOperation<String> migrationOp = done(OP_NAME, "");

    private static final CompositeType TYPE;

    static {
        CompositeType type;
        try {
            type = new CompositeType("BlobMigrationStatus", "Status of the blob migraiton",
                    new String[] { "isRunning", "migratedNodes", "lastProcessedPath", "operationStatus" },
                    new String[] { "Migration in progress", "Total number of migrated nodes", "Last processed path", "Status of the operation" },
                    new OpenType[] { SimpleType.BOOLEAN, SimpleType.INTEGER, SimpleType.STRING, ManagementOperation.Status.ITEM_TYPES });
        } catch (OpenDataException e) {
            type = null;
            log.error("Can't create a CompositeType", e);
        }
        TYPE = type;
    }

    @Reference(target = "(service.pid=org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore)")
    private BlobStore splitBlobStore;

    @Reference
    private NodeStore nodeStore;

    private Executor executor = Executors.newSingleThreadExecutor();

    private BlobMigrator migrator;

    private Registration mbeanReg;

    public BlobMigration() {
        super(BlobMigrationMBean.class);
    }

    @Activate
    private void activate(BundleContext ctx) {
        Whiteboard wb = new OsgiWhiteboard(ctx);
        migrator = new BlobMigrator((SplitBlobStore) splitBlobStore, nodeStore);
        mbeanReg = registerMBean(wb, BlobMigrationMBean.class, this, BlobMigrationMBean.TYPE, OP_NAME);
    }

    @Deactivate
    private void deactivate() throws InterruptedException {
        if (migrator != null) {
            migrator.stop();
            migrator = null;
        }
        if (mbeanReg != null) {
            mbeanReg.unregister();
            mbeanReg = null;
        }
    }

    @Nonnull
    @Override
    public String startBlobMigration(final boolean resume) {
        if (migrationOp.isDone()) {
            migrationOp = newManagementOperation(OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    boolean finished;
                    if (resume) {
                        finished = migrator.migrate();
                    } else {
                        finished = migrator.start();
                    }
                    String duration = formatTime(nanoTime() - t0);
                    if (finished) {
                        return "All blobs migrated in " + duration;
                    } else {
                        return "Migration stopped manually after " + duration;
                    }
                }
            });
            executor.execute(migrationOp);
            return "Migration started";
        } else {
            return "Migration is already in progress";
        }
    }

    @Nonnull
    @Override
    public String stopBlobMigration() {
        migrator.stop();
        return "Migration will be stopped";
    }

    @Nonnull
    @Override
    public CompositeData getBlobMigrationStatus() throws OpenDataException {
        Map<String, Object> status = new HashMap<String, Object>();
        status.put("isRunning", migrationOp.getStatus().getCode() == StatusCode.RUNNING);
        status.put("migratedNodes", migrator.getTotalMigratedNodes());
        status.put("lastProcessedPath", migrator.getLastProcessedPath());
        status.put("operationStatus", migrationOp.getStatus().toCompositeData());
        return new CompositeDataSupport(TYPE, status);
    }
}
