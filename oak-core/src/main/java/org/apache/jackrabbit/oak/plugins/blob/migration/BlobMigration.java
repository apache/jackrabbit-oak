package org.apache.jackrabbit.oak.plugins.blob.migration;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.nanoTime;
import static org.apache.jackrabbit.oak.management.ManagementOperation.done;
import static org.apache.jackrabbit.oak.management.ManagementOperation.newManagementOperation;
import static org.apache.jackrabbit.oak.management.ManagementOperation.Status.formatTime;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.management.ManagementOperation;

public class BlobMigration implements BlobMigrationMBean {

    public static final String OP_NAME = "Blob migration";

    private final BlobMigrator blobMigrator;

    private final Executor executor;

    private ManagementOperation<String> migrationOp = done(OP_NAME, "");

    public BlobMigration(@Nonnull BlobMigrator blobMigrator, @Nonnull Executor executor) {
        this.blobMigrator = checkNotNull(blobMigrator);
        this.executor = checkNotNull(executor);
    }

    @Nonnull
    @Override
    public CompositeData startBlobGC() {
        if (migrationOp.isDone()) {
            migrationOp = newManagementOperation(OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    blobMigrator.migrate();
                    return "All blobs migrated in " + formatTime(nanoTime() - t0);
                }
            });
            executor.execute(migrationOp);
        }
        return getBlobGCStatus();
    }

    @Nonnull
    @Override
    public CompositeData getBlobGCStatus() {
        return migrationOp.getStatus().toCompositeData();
    }
}
