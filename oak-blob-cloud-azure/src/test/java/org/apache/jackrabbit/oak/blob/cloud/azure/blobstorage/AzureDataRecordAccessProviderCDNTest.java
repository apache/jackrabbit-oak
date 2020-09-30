/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.net.URI;
import java.util.Properties;

import com.google.common.base.Strings;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.api.blob.BlobUploadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AzureDataRecordAccessProviderCDNTest extends AzureDataRecordAccessProviderTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static AzureDataStore cdnDataStore;

    private static String DOWNLOAD_URI_DOMAIN = AzureDataStoreUtils
            .getDirectAccessDataStoreProperties()
            .getProperty(PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE, null);
    private static String UPLOAD_URI_DOMAIN = AzureDataStoreUtils
            .getDirectAccessDataStoreProperties()
            .getProperty(PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE, null);

    private static String cdnSetupNotice = String.format(
            "%s\n%s %s '%s' %s '%s' %s %s",
            "No override domains configured - skipping Azure CDN tests.",
            "To run these tests, set up an Azure CDN in the Azure console or command line,",
            "then set the CDN domain as the property value for",
            PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE,
            "and/or",
            PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE,
            "in your Azure configuration file, and then provide this file to the",
            "test via the -Dazure.config command-line switch"
    );

    @BeforeClass
    public static void setupDataStore() throws Exception {
        assumeTrue(cdnSetupNotice, isCDNConfigured());
        cdnDataStore = AzureDataStoreUtils.setupDirectAccessDataStore(homeDir,
                expirySeconds, expirySeconds);
    }

    private static boolean isCDNConfigured() {
        return ! Strings.isNullOrEmpty(DOWNLOAD_URI_DOMAIN) && ! Strings.isNullOrEmpty(UPLOAD_URI_DOMAIN);
    }

    private static AzureDataStore createDataStore(@NotNull Properties properties) throws Exception {
        return AzureDataStoreUtils.setupDirectAccessDataStore(homeDir, expirySeconds, expirySeconds, properties);
    }

    @Override
    protected ConfigurableDataRecordAccessProvider getDataStore() {
        return cdnDataStore;
    }

    @Override
    protected ConfigurableDataRecordAccessProvider getDataStore(@NotNull Properties overrideProperties) throws Exception {
        return createDataStore(AzureDataStoreUtils.getDirectAccessDataStoreProperties(overrideProperties));
    }

    private ConfigurableDataRecordAccessProvider getCDNEnabledDataStore() throws Exception {
        Properties properties = new Properties();
        properties.put(PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS, "false");
        return getDataStore(properties);
    }

    // CDN Tests
    @Test
    public void testCDNDownloadURIContainsDownloadDomain() throws Exception {
        ConfigurableDataRecordAccessProvider ds = getCDNEnabledDataStore();
        DataIdentifier id = new DataIdentifier("identifier");
        URI downloadUri = ds.getDownloadURI(id, DataRecordDownloadOptions.DEFAULT);
        assertNotNull(downloadUri);
        assertEquals(DOWNLOAD_URI_DOMAIN, downloadUri.getHost());
    }

    @Test
    public void testCDNUploadURIContainsUploadDomain() throws Exception {
        Properties properties = new Properties();
        ConfigurableDataRecordAccessProvider ds = getDataStore(properties);
        DataRecordUpload upload = ds.initiateDataRecordUpload(ONE_MB, 10);
        assertNotNull(upload);
        assertTrue(upload.getUploadURIs().size() > 0);
        for (URI uri : upload.getUploadURIs()) {
            assertEquals(UPLOAD_URI_DOMAIN, uri.getHost());
        }
    }

    @Test
    public void testVetoDownloadDomainOverride() throws Exception {
        ConfigurableDataRecordAccessProvider ds = getCDNEnabledDataStore();
        DataIdentifier id = new DataIdentifier("identifier");
        DataRecordDownloadOptions options =
                DataRecordDownloadOptions.fromBlobDownloadOptions(
                        new BlobDownloadOptions(null, null, null, "inline", true)
                );
        URI downloadUri = ds.getDownloadURI(id, options);
        assertNotNull(downloadUri);
        assertNotEquals(DOWNLOAD_URI_DOMAIN, downloadUri.getHost());

        Properties properties = AzureDataStoreUtils.getDirectAccessDataStoreProperties();
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, null);
        assertNotNull(accountName);
        assertEquals(String.format("%s.blob.core.windows.net", accountName), downloadUri.getHost());
    }

    @Test
    public void testVetoUploadDomainOverride() throws Exception {
        ConfigurableDataRecordAccessProvider ds = getCDNEnabledDataStore();
        DataRecordUpload upload = ds.initiateDataRecordUpload(ONE_MB, 10,
                DataRecordUploadOptions.fromBlobUploadOptions(new BlobUploadOptions(true)));
        assertNotNull(upload);
        assertTrue(upload.getUploadURIs().size() > 0);

        Properties properties = AzureDataStoreUtils.getDirectAccessDataStoreProperties();
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, null);
        assertNotNull(accountName);
        String defaultDomain = String.format("%s.blob.core.windows.net", accountName);
        for (URI uri : upload.getUploadURIs()) {
            assertEquals(defaultDomain, uri.getHost());
        }
    }
}
