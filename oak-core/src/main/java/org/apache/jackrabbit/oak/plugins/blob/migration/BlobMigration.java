package org.apache.jackrabbit.oak.plugins.blob.migration;

import static java.lang.System.nanoTime;
import static org.apache.jackrabbit.oak.management.ManagementOperation.done;
import static org.apache.jackrabbit.oak.management.ManagementOperation.newManagementOperation;
import static org.apache.jackrabbit.oak.management.ManagementOperation.Status.formatTime;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.management.ManagementOperation;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;

@Component
public class BlobMigration implements BlobMigrationMBean {

    public static final String OP_NAME = "Blob migration";

    private ManagementOperation<String> migrationOp = done(OP_NAME, "");

    @Reference(target="(service.pid=org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore)")
    private BlobStore splitBlobStore;

    @Reference
    private NodeStore nodeStore;

    @Reference
    private Executor executor;

    private BlobMigrator migrator;

    private Registration mbeanReg;

    @Activate
    private void activate(BundleContext ctx) {
        final Whiteboard wb = new OsgiWhiteboard(ctx);
        migrator = new BlobMigrator((SplitBlobStore) splitBlobStore, nodeStore);
        mbeanReg = registerMBean(wb,
                BlobMigrationMBean.class,
                this,
                BlobMigrationMBean.TYPE,
                OP_NAME);
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
    public CompositeData startBlobMigration() {
        if (migrationOp.isDone()) {
            migrationOp = newManagementOperation(OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    migrator.migrate();
                    return "All blobs migrated in " + formatTime(nanoTime() - t0);
                }
            });
            executor.execute(migrationOp);
        }
        return getBlobMigrationStatus();
    }

    @Nonnull
    @Override
    public CompositeData getBlobMigrationStatus() {
        return migrationOp.getStatus().toCompositeData();
    }
}
