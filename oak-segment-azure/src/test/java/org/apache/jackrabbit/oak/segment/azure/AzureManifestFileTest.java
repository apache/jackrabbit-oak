package org.apache.jackrabbit.oak.segment.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AzureManifestFileTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
    }

    @Test
    public void testManifest() throws URISyntaxException, IOException {
        ManifestFile manifestFile = new AzurePersistence(container.getDirectoryReference("oak")).getManifestFile();
        assertFalse(manifestFile.exists());

        Properties props = new Properties();
        props.setProperty("xyz", "abc");
        props.setProperty("version", "123");
        manifestFile.save(props);

        Properties loaded = manifestFile.load();
        assertEquals(props, loaded);
    }

}
