package org.apache.jackrabbit.oak.plugins.blob.migration;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

public interface BlobMigrationMBean {
    String TYPE = "BlobMigration";

    @Nonnull
    CompositeData startBlobMigration();

    @Nonnull
    CompositeData getBlobMigrationStatus();

}
