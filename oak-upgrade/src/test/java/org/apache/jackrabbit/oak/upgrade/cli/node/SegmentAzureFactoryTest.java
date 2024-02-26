package org.apache.jackrabbit.oak.upgrade.cli.node;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.SharedAccessAccountPermissions;
import com.microsoft.azure.storage.SharedAccessAccountPolicy;
import com.microsoft.azure.storage.SharedAccessAccountResourceType;
import com.microsoft.azure.storage.SharedAccessAccountService;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.apache.jackrabbit.oak.upgrade.cli.CliUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_ID;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_SECRET;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_SECRET_KEY;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class SegmentAzureFactoryTest {

    @ClassRule
    public static final AzuriteDockerRule azurite = new AzuriteDockerRule();

    private static final Environment ENVIRONMENT = new Environment();
    private static final String CONTAINER_NAME = "oak-test";
    private static final String DIR = "repository";


    @Test
    public void testConnectionWithConnectionString_accessKey() throws IOException {
        String connectionStringWithPlaceholder = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;BlobEndpoint=http://127.0.0.1:%s/%s;";
        String connectionString = String.format(connectionStringWithPlaceholder, AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, azurite.getMappedPort(), AzuriteDockerRule.ACCOUNT_NAME);
        SegmentAzureFactory segmentAzureFactory = new SegmentAzureFactory.Builder("respository", 256,
                false)
                .connectionString(connectionString)
                .containerName(CONTAINER_NAME)
                .build();
        Closer closer = Closer.create();
        CliUtils.handleSigInt(closer);
        FileStoreUtils.NodeStoreWithFileStore nodeStore = (FileStoreUtils.NodeStoreWithFileStore) segmentAzureFactory.create(null, closer);
        assertEquals(1, nodeStore.getFileStore().getSegmentCount());
        closer.close();
    }

    @Test
    public void testConnectionWithConnectionString_sas() throws IOException {
        String sasToken = getAccountSasToken();
        String connectionStringWithPlaceholder = "DefaultEndpointsProtocol=http;AccountName=%s;SharedAccessSignature=%s;BlobEndpoint=http://127.0.0.1:%s/%s;";
        String connectionString = String.format(connectionStringWithPlaceholder, AzuriteDockerRule.ACCOUNT_NAME, sasToken, azurite.getMappedPort(), AzuriteDockerRule.ACCOUNT_NAME);
        SegmentAzureFactory segmentAzureFactory = new SegmentAzureFactory.Builder(DIR, 256,
                false)
                .connectionString(connectionString)
                .containerName(CONTAINER_NAME)
                .build();
        Closer closer = Closer.create();
        CliUtils.handleSigInt(closer);
        FileStoreUtils.NodeStoreWithFileStore nodeStore = (FileStoreUtils.NodeStoreWithFileStore) segmentAzureFactory.create(null, closer);
        assertEquals(1, nodeStore.getFileStore().getSegmentCount());
        closer.close();
    }

    /* if AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET are already set in environment variables
     * then they will be given preference and authentication will be done via service principals and this
     * test will be skipped
     * */
    @Test
    public void testConnectionWithUri_accessKey() throws IOException {
        assumeTrue(StringUtils.isBlank(ENVIRONMENT.getVariable(AZURE_TENANT_ID)));
        assumeTrue(StringUtils.isBlank(ENVIRONMENT.getVariable(AZURE_CLIENT_ID)));
        assumeTrue(StringUtils.isBlank(ENVIRONMENT.getVariable(AZURE_CLIENT_SECRET)));

        assumeNotNull(ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_SECRET_KEY));

        final String CONTAINER_NAME = "oak-migration-test";

        String uri = String.format("https://%s.blob.core.windows.net/%s", ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME), CONTAINER_NAME);
        Closer closer = Closer.create();
        try {
            SegmentAzureFactory segmentAzureFactory = new SegmentAzureFactory.Builder(DIR, 256,
                    false)
                    .accountName(ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME))
                    .uri(uri)
                    .build();
            closer = Closer.create();
            CliUtils.handleSigInt(closer);
            FileStoreUtils.NodeStoreWithFileStore nodeStore = (FileStoreUtils.NodeStoreWithFileStore) segmentAzureFactory.create(null, closer);
            assertEquals(1, nodeStore.getFileStore().getSegmentCount());
        } finally {
            closer.close();
            cleanup(uri);
        }
    }

    @Test
    public void testConnectionWithUri_servicePrincipal() throws IOException, InterruptedException {
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_TENANT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_CLIENT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_CLIENT_SECRET));

        final String CONTAINER_NAME = "oak-migration-test";

        String uri = String.format("https://%s.blob.core.windows.net/%s", ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME), CONTAINER_NAME);
        Closer closer = Closer.create();
        try {
            SegmentAzureFactory segmentAzureFactory = new SegmentAzureFactory.Builder(DIR, 256,
                    false)
                    .accountName(ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME))
                    .uri(uri)
                    .build();

            CliUtils.handleSigInt(closer);
            FileStoreUtils.NodeStoreWithFileStore nodeStore = (FileStoreUtils.NodeStoreWithFileStore) segmentAzureFactory.create(null, closer);
            assertEquals(1, nodeStore.getFileStore().getSegmentCount());
        } finally {
            closer.close();
            cleanup(uri);
        }
    }

    private void cleanup(String uri) {
        uri = uri + "/" + DIR;
        try {
            CloudBlobDirectory cloudBlobDirectory = ToolUtils.createCloudBlobDirectory(uri, ENVIRONMENT);
            AzureUtilities.deleteAllBlobs(cloudBlobDirectory);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    @NotNull
    private String getAccountSasToken() {
        try {
            CloudStorageAccount cloudStorageAccount = azurite.getCloudStorageAccount();
            return cloudStorageAccount.generateSharedAccessSignature(getPolicy());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private SharedAccessAccountPolicy getPolicy() {
        SharedAccessAccountPolicy sharedAccessAccountPolicy = new SharedAccessAccountPolicy();
        EnumSet<SharedAccessAccountPermissions> sharedAccessAccountPermissions = EnumSet.of(SharedAccessAccountPermissions.CREATE,
                SharedAccessAccountPermissions.DELETE, SharedAccessAccountPermissions.READ, SharedAccessAccountPermissions.UPDATE,
                SharedAccessAccountPermissions.WRITE, SharedAccessAccountPermissions.LIST);
        EnumSet<SharedAccessAccountService> sharedAccessAccountServices = EnumSet.of(SharedAccessAccountService.BLOB);
        EnumSet<SharedAccessAccountResourceType> sharedAccessAccountResourceTypes = EnumSet.of(
                SharedAccessAccountResourceType.CONTAINER, SharedAccessAccountResourceType.OBJECT, SharedAccessAccountResourceType.SERVICE);

        sharedAccessAccountPolicy.setPermissions(sharedAccessAccountPermissions);
        sharedAccessAccountPolicy.setServices(sharedAccessAccountServices);
        sharedAccessAccountPolicy.setResourceTypes(sharedAccessAccountResourceTypes);
        sharedAccessAccountPolicy.setSharedAccessExpiryTime(Date.from(Instant.now().plus(Duration.ofDays(7))));
        return sharedAccessAccountPolicy;
    }

}
