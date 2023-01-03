/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.azure;

import com.google.common.collect.ImmutableSet;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.StreamSupport;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.osgi.util.converter.Converters;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.*;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public class AzureSegmentStoreServiceTest {
    
    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public final OsgiContext context = new OsgiContext();

    private static final EnumSet<SharedAccessBlobPermissions> READ_ONLY = EnumSet.of(READ, LIST);
    private static final EnumSet<SharedAccessBlobPermissions> READ_WRITE = EnumSet.of(READ, LIST, CREATE, WRITE, ADD);
    private static final ImmutableSet<String> BLOBS = ImmutableSet.of("blob1", "blob2");
    
    private CloudBlobContainer container;
    
    @Before
    public void setup() throws Exception {
        container = azurite.getContainer(AzureSegmentStoreService.DEFAULT_CONTAINER_NAME);
        for (String blob : BLOBS) {
            container.getBlockBlobReference(blob + ".txt").uploadText(blob);
        }
    }

    @Test
    public void connectWithSharedAccessSignatureURL_readOnly() throws Exception {
        String sasToken = container.generateSharedAccessSignature(policy(READ_ONLY), null);

        AzureSegmentStoreService azureSegmentStoreService = new AzureSegmentStoreService();
        azureSegmentStoreService.activate(context.componentContext(), getConfigurationWithSharedAccessSignature(sasToken));

        SegmentNodeStorePersistence persistence = context.getService(SegmentNodeStorePersistence.class);
        assertNotNull(persistence);
        assertWriteAccessNotGranted(persistence);
        assertReadAccessGranted(persistence, BLOBS);
    }

    @Test
    public void connectWithSharedAccessSignatureURL_readWrite() throws Exception {
        String sasToken = container.generateSharedAccessSignature(policy(READ_WRITE), null);

        AzureSegmentStoreService azureSegmentStoreService = new AzureSegmentStoreService();
        azureSegmentStoreService.activate(context.componentContext(), getConfigurationWithSharedAccessSignature(sasToken));

        SegmentNodeStorePersistence persistence = context.getService(SegmentNodeStorePersistence.class);
        assertNotNull(persistence);
        assertWriteAccessGranted(persistence);
        assertReadAccessGranted(persistence, concat(BLOBS, "test"));
    }

    @Test
    public void connectWithSharedAccessSignatureURL_expired() throws Exception {
        SharedAccessBlobPolicy expiredPolicy = policy(READ_WRITE, yesterday());
        String sasToken = container.generateSharedAccessSignature(expiredPolicy, null);

        AzureSegmentStoreService azureSegmentStoreService = new AzureSegmentStoreService();
        azureSegmentStoreService.activate(context.componentContext(), getConfigurationWithSharedAccessSignature(sasToken));

        SegmentNodeStorePersistence persistence = context.getService(SegmentNodeStorePersistence.class);
        assertNotNull(persistence);
        assertWriteAccessNotGranted(persistence);
        assertReadAccessNotGranted(persistence);
    }

    @Test
    public void connectWithAccessKey() throws Exception {
        AzureSegmentStoreService azureSegmentStoreService = new AzureSegmentStoreService();
        azureSegmentStoreService.activate(context.componentContext(), getConfigurationWithAccessKey(AzuriteDockerRule.ACCOUNT_KEY));

        SegmentNodeStorePersistence persistence = context.getService(SegmentNodeStorePersistence.class);
        assertNotNull(persistence);
        assertWriteAccessGranted(persistence);
        assertReadAccessGranted(persistence, concat(BLOBS, "test"));
    }

    @Test
    public void connectWithConnectionURL() throws Exception {
        AzureSegmentStoreService azureSegmentStoreService = new AzureSegmentStoreService();
        azureSegmentStoreService.activate(context.componentContext(), getConfigurationWithConfigurationURL(AzuriteDockerRule.ACCOUNT_KEY));

        SegmentNodeStorePersistence persistence = context.getService(SegmentNodeStorePersistence.class);
        assertNotNull(persistence);
        assertWriteAccessGranted(persistence);
        assertReadAccessGranted(persistence, concat(BLOBS, "test"));
    }

    @Test
    public void deactivate() throws Exception {
        AzureSegmentStoreService azureSegmentStoreService = new AzureSegmentStoreService();
        azureSegmentStoreService.activate(context.componentContext(), getConfigurationWithAccessKey(AzuriteDockerRule.ACCOUNT_KEY));
        assertNotNull(context.getService(SegmentNodeStorePersistence.class));

        azureSegmentStoreService.deactivate();
        assertNull(context.getService(SegmentNodeStorePersistence.class));
    }

    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions, Instant expirationTime) {
        SharedAccessBlobPolicy sharedAccessBlobPolicy = new SharedAccessBlobPolicy();
        sharedAccessBlobPolicy.setPermissions(permissions);
        sharedAccessBlobPolicy.setSharedAccessExpiryTime(Date.from(expirationTime));
        return sharedAccessBlobPolicy;
    }

    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions) {
        return policy(permissions, Instant.now().plus(Duration.ofDays(7)));
    }

    private static void assertReadAccessGranted(SegmentNodeStorePersistence persistence, Set<String> expectedBlobs) throws Exception {
        CloudBlobContainer container = getContainerFrom(persistence);
        Set<String> actualBlobNames = StreamSupport.stream(container.listBlobs().spliterator(), false)
            .map(blob -> blob.getUri().getPath())
            .map(path -> path.substring(path.lastIndexOf('/') + 1))
            .collect(toSet());
        Set<String> expectedBlobNames = expectedBlobs.stream().map(name -> name + ".txt").collect(toSet());

        assertEquals(expectedBlobNames, actualBlobNames);

        Set<String> actualBlobContent = actualBlobNames.stream()
            .map(name -> {
                try {
                    return container.getBlockBlobReference(name).downloadText();
                } catch (StorageException | IOException | URISyntaxException e) {
                    throw new RuntimeException("Error while reading blob " + name, e);
                }
            })
            .collect(toSet());
        assertEquals(expectedBlobs, actualBlobContent);
    }

    private static void assertWriteAccessGranted(SegmentNodeStorePersistence persistence) throws Exception {
        getContainerFrom(persistence)
            .getBlockBlobReference("test.txt").uploadText("test");
    }

    private static CloudBlobContainer getContainerFrom(SegmentNodeStorePersistence persistence) throws Exception {
        return ((AzurePersistence) persistence).getSegmentstoreDirectory().getContainer();
    }

    private static void assertWriteAccessNotGranted(SegmentNodeStorePersistence persistence) {
        try {
            assertWriteAccessGranted(persistence);
            fail("Write access should not be granted, but writing to the storage succeeded.");
        } catch (Exception e) {
            // successful
        }
    }

    private static void assertReadAccessNotGranted(SegmentNodeStorePersistence persistence) {
        try {
            assertReadAccessGranted(persistence, BLOBS);
            fail("Read access should not be granted, but reading from the storage succeeded.");
        } catch (Exception e) {
            // successful
        }
    }

    private static Instant yesterday() {
        return Instant.now().minus(Duration.ofDays(1));
    }
    
    private static ImmutableSet<String> concat(ImmutableSet<String> blobs, String element) {
        return ImmutableSet.<String>builder().addAll(blobs).add(element).build();
    }

    private static Configuration getConfigurationWithSharedAccessSignature(String sasToken) {
        return getConfiguration(sasToken, null, null);
    }

    private static Configuration getConfigurationWithAccessKey(String accessKey) {
        return getConfiguration(null, accessKey, null);
    }

    private static Configuration getConfigurationWithConfigurationURL(String accessKey) {
        String connectionString = "DefaultEndpointsProtocol=https;"
            + "BlobEndpoint=" + azurite.getBlobEndpoint() + ';'
            + "AccountName=" + AzuriteDockerRule.ACCOUNT_NAME + ';'
            + "AccountKey=" + accessKey + ';';
        return getConfiguration(null, null, connectionString);
    }

    @NotNull
    private static Configuration getConfiguration(String sasToken, String accessKey, String connectionURL) {
        return Converters.standardConverter()
                .convert(new HashMap<Object, Object>() {{
                    put("accountName", AzuriteDockerRule.ACCOUNT_NAME);
                    put("accessKey", accessKey);
                    put("connectionURL", connectionURL);
                    put("sharedAccessSignature", sasToken);
                    put("blobEndpoint", azurite.getBlobEndpoint());
                }})
                .to(Configuration.class);
    }
}
