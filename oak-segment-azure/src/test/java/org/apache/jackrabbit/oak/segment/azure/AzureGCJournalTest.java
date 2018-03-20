package org.apache.jackrabbit.oak.segment.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.file.GcJournalTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureGCJournalTest extends GcJournalTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
    }

    @Override
    protected SegmentNodeStorePersistence getPersistence() throws Exception {
        return new AzurePersistence(container.getDirectoryReference("oak"));
    }

    @Test
    @Ignore
    @Override
    public void testReadOak16GCLog() throws Exception {
        super.testReadOak16GCLog();
    }

    @Test
    @Ignore
    @Override
    public void testUpdateOak16GCLog() throws Exception {
        super.testUpdateOak16GCLog();
    }
}
