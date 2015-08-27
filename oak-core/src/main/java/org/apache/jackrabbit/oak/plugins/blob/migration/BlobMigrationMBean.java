package org.apache.jackrabbit.oak.plugins.blob.migration;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import org.apache.jackrabbit.oak.commons.jmx.Description;
import org.apache.jackrabbit.oak.commons.jmx.Name;

public interface BlobMigrationMBean {

    String TYPE = "BlobMigration";

    @Nonnull
    @Description("Start or resume the blob migration")
    String startBlobMigration(
            @Name("resume") @Description("true to resume stopped migration or false to start it from scratch") boolean resume);

    @Nonnull
    @Description("Stop the blob migration")
    String stopBlobMigration();

    @Nonnull
    CompositeData getBlobMigrationStatus() throws OpenDataException;

}
